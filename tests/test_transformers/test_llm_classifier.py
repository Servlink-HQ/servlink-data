"""
Unit tests for the LLM classifier transformer.

All external dependencies (Ollama, Supabase, settings) are mocked so
these tests run fully offline without any real credentials or models.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest

from src.models.estabelecimento import ClassifiedEstablishment, EstablishmentType, RawEstablishment
from src.transformers.llm_classifier import (
    _BATCH_SIZE,
    _build_dim_row,
    _build_prompt,
    _classify_with_llm,
    _insert_dim_record,
    _is_duplicate,
    _log_pipeline,
    _mark_processed,
    run_classification,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_raw(**kwargs) -> RawEstablishment:
    defaults = dict(
        source="overpass",
        source_id="node/1",
        name="Hotel Beira Mar",
        latitude=-27.59,
        longitude=-48.54,
        raw_type="tourism=hotel",
    )
    defaults.update(kwargs)
    return RawEstablishment(**defaults)


def _make_classified(**kwargs) -> ClassifiedEstablishment:
    defaults = dict(
        standardized_name="Hotel Beira Mar",
        type=EstablishmentType.HOTEL,
        subtype="boutique_hotel",
        suggested_cnae="5510-8/01",
        estimated_neighborhood="Centro",
        tags=["wifi", "ocean_view"],
        confidence=0.95,
    )
    defaults.update(kwargs)
    return ClassifiedEstablishment(**defaults)


def _make_raw_row(
    record_id: str = "uuid-1",
    payload: dict | None = None,
) -> dict:
    raw = _make_raw()
    return {
        "id": record_id,
        "source": "overpass",
        "source_id": "node/1",
        "payload": payload if payload is not None else raw.model_dump(mode="json"),
        "processed": False,
        "batch_id": "batch-abc",
    }


def _classified_json(**kwargs) -> str:
    return _make_classified(**kwargs).model_dump_json()


def _make_mock_supabase(
    raw_rows: list[dict] | None = None,
    insert_data: list[dict] | None = None,
    rpc_data: str | None = None,
) -> MagicMock:
    client = MagicMock()

    # raw_crawled_data SELECT
    client.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value.data = (
        raw_rows if raw_rows is not None else []
    )

    # dim_estabelecimentos INSERT
    client.table.return_value.insert.return_value.execute.return_value.data = (
        insert_data if insert_data is not None else [{"id": "dim-uuid-1"}]
    )

    # data_pipeline_logs INSERT
    client.table.return_value.insert.return_value.execute.return_value.data = []

    # RPC dedup check
    client.rpc.return_value.execute.return_value.data = rpc_data

    return client


def _make_ollama_response(content: str) -> MagicMock:
    mock_resp = MagicMock()
    mock_resp.message.content = content
    return mock_resp


# ---------------------------------------------------------------------------
# _build_prompt
# ---------------------------------------------------------------------------

class TestBuildPrompt:
    def test_includes_name_and_type(self) -> None:
        raw = _make_raw(name="Restaurante do Porto", raw_type="amenity=restaurant")
        prompt = _build_prompt(raw)
        assert "Restaurante do Porto" in prompt
        assert "amenity=restaurant" in prompt

    def test_includes_coordinates(self) -> None:
        raw = _make_raw(latitude=-27.59, longitude=-48.54)
        prompt = _build_prompt(raw)
        assert "-27.59" in prompt
        assert "-48.54" in prompt

    def test_includes_optional_fields_when_present(self) -> None:
        raw = _make_raw(
            address="Rua das Flores, 10",
            cuisine="seafood",
            phone="+55 48 3333-0000",
            website="https://example.com",
            opening_hours="Mo-Su 11:00-23:00",
        )
        prompt = _build_prompt(raw)
        assert "Rua das Flores" in prompt
        assert "seafood" in prompt
        assert "+55 48 3333-0000" in prompt
        assert "https://example.com" in prompt
        assert "Mo-Su 11:00-23:00" in prompt

    def test_omits_none_optional_fields(self) -> None:
        raw = _make_raw(address=None, cuisine=None, phone=None)
        prompt = _build_prompt(raw)
        assert "Address:" not in prompt
        assert "Cuisine:" not in prompt
        assert "Phone:" not in prompt

    def test_includes_useful_extra_data(self) -> None:
        raw = _make_raw(extra_data={"stars": "4", "wheelchair": "yes", "irrelevant": "x"})
        prompt = _build_prompt(raw)
        assert "stars" in prompt
        assert "wheelchair" in prompt

    def test_unknown_name_shown_as_unknown(self) -> None:
        raw = _make_raw(name=None)
        prompt = _build_prompt(raw)
        assert "(unknown)" in prompt


# ---------------------------------------------------------------------------
# _classify_with_llm
# ---------------------------------------------------------------------------

class TestClassifyWithLlm:
    def test_returns_classified_establishment_on_success(self) -> None:
        raw = _make_raw()
        content = _classified_json()
        mock_resp = _make_ollama_response(content)

        with patch("src.transformers.llm_classifier.ollama.Client") as MockClient:
            MockClient.return_value.chat.return_value = mock_resp
            result = _classify_with_llm(raw, "http://localhost:11434", "llama3.1:8b")

        assert result.standardized_name == "Hotel Beira Mar"
        assert result.type == EstablishmentType.HOTEL
        assert result.confidence == pytest.approx(0.95)

    def test_passes_schema_as_format(self) -> None:
        raw = _make_raw()
        mock_resp = _make_ollama_response(_classified_json())

        with patch("src.transformers.llm_classifier.ollama.Client") as MockClient:
            MockClient.return_value.chat.return_value = mock_resp
            _classify_with_llm(raw, "http://localhost:11434", "llama3.1:8b")

        chat_kwargs = MockClient.return_value.chat.call_args[1]
        assert "format" in chat_kwargs
        assert "standardized_name" in str(chat_kwargs["format"])

    def test_passes_system_and_user_messages(self) -> None:
        raw = _make_raw()
        mock_resp = _make_ollama_response(_classified_json())

        with patch("src.transformers.llm_classifier.ollama.Client") as MockClient:
            MockClient.return_value.chat.return_value = mock_resp
            _classify_with_llm(raw, "http://localhost:11434", "llama3.1:8b")

        messages = MockClient.return_value.chat.call_args[1]["messages"]
        roles = [m["role"] for m in messages]
        assert "system" in roles
        assert "user" in roles

    def test_wraps_connection_error_for_retry(self) -> None:
        raw = _make_raw()

        with (
            patch("src.transformers.llm_classifier.ollama.Client") as MockClient,
            patch("time.sleep"),
            pytest.raises(ConnectionError, match="Ollama"),
        ):
            MockClient.return_value.chat.side_effect = Exception("connection refused")
            _classify_with_llm(raw, "http://localhost:11434", "llama3.1:8b")

    def test_retries_on_connection_error(self) -> None:
        raw = _make_raw()
        content = _classified_json()
        mock_resp = _make_ollama_response(content)

        with (
            patch("src.transformers.llm_classifier.ollama.Client") as MockClient,
            patch("time.sleep"),
        ):
            MockClient.return_value.chat.side_effect = [
                Exception("connection refused"),
                Exception("connection refused"),
                mock_resp,
            ]
            result = _classify_with_llm(raw, "http://localhost:11434", "llama3.1:8b")

        assert result.standardized_name == "Hotel Beira Mar"

    def test_raises_value_error_on_invalid_json(self) -> None:
        raw = _make_raw()
        mock_resp = _make_ollama_response("not valid json at all")

        with (
            patch("src.transformers.llm_classifier.ollama.Client") as MockClient,
            pytest.raises(Exception),
        ):
            MockClient.return_value.chat.return_value = mock_resp
            _classify_with_llm(raw, "http://localhost:11434", "llama3.1:8b")


# ---------------------------------------------------------------------------
# _is_duplicate
# ---------------------------------------------------------------------------

class TestIsDuplicate:
    def test_returns_none_when_no_duplicate(self) -> None:
        client = MagicMock()
        client.rpc.return_value.execute.return_value.data = None
        assert _is_duplicate(client, "Hotel X", -27.59, -48.54) is None

    def test_returns_uuid_when_duplicate_found(self) -> None:
        client = MagicMock()
        client.rpc.return_value.execute.return_value.data = "existing-dim-uuid"
        result = _is_duplicate(client, "Hotel X", -27.59, -48.54)
        assert result == "existing-dim-uuid"

    def test_calls_correct_rpc_function(self) -> None:
        client = MagicMock()
        client.rpc.return_value.execute.return_value.data = None
        _is_duplicate(client, "Hotel Teste", -27.5, -48.5)
        client.rpc.assert_called_once_with(
            "check_establishment_duplicate",
            {"p_nome": "Hotel Teste", "p_lat": -27.5, "p_lon": -48.5},
        )

    def test_returns_none_when_rpc_fails(self) -> None:
        client = MagicMock()
        client.rpc.side_effect = Exception("function not found")
        # Must not raise — graceful degradation
        assert _is_duplicate(client, "Hotel X", -27.59, -48.54) is None


# ---------------------------------------------------------------------------
# _build_dim_row
# ---------------------------------------------------------------------------

class TestBuildDimRow:
    def test_maps_all_standard_fields(self) -> None:
        raw = _make_raw(
            phone="+55 48 9999-0000",
            website="https://hotel.com",
            opening_hours="Mo-Su 08:00-22:00",
            address="Rua A, 1",
            rating=4.5,
            review_count=100,
        )
        classified = _make_classified(
            standardized_name="Hotel Beira Mar",
            type=EstablishmentType.HOTEL,
            subtype="boutique_hotel",
            suggested_cnae="5510-8/01",
            estimated_neighborhood="Centro",
            tags=["wifi", "pool"],
            confidence=0.95,
        )
        row = _build_dim_row(raw, classified)

        assert row["nome"] == "Hotel Beira Mar"
        assert row["nome_original"] == "Hotel Beira Mar"
        assert row["tipo"] == "hotel"
        assert row["subtipo"] == "boutique_hotel"
        assert row["cnae_codigo"] == "5510-8/01"
        assert row["bairro"] == "Centro"
        assert row["telefone"] == "+55 48 9999-0000"
        assert row["website"] == "https://hotel.com"
        assert row["horario_funcionamento"] == "Mo-Su 08:00-22:00"
        assert row["endereco"] == "Rua A, 1"
        assert row["rating_google"] == pytest.approx(4.5)
        assert row["total_reviews"] == 100
        assert row["ativo"] is True

    def test_location_is_wkt_point(self) -> None:
        raw = _make_raw(latitude=-27.59, longitude=-48.54)
        row = _build_dim_row(raw, _make_classified())
        assert row["location"] == "SRID=4326;POINT(-48.54 -27.59)"

    def test_cuisine_split_from_semicolons(self) -> None:
        raw = _make_raw(cuisine="italian;seafood;brazilian")
        row = _build_dim_row(raw, _make_classified())
        assert row["cuisine"] == ["italian", "seafood", "brazilian"]

    def test_cuisine_none_when_not_provided(self) -> None:
        raw = _make_raw(cuisine=None)
        row = _build_dim_row(raw, _make_classified())
        assert row["cuisine"] is None

    def test_source_refs_osm_id_for_overpass(self) -> None:
        raw = _make_raw(source="overpass", source_id="node/12345")
        row = _build_dim_row(raw, _make_classified())
        assert row["source_refs"] == {"osm_id": "node/12345"}

    def test_source_refs_uses_source_name_for_other(self) -> None:
        raw = _make_raw(source="outscraper", source_id="ChIJ123")
        row = _build_dim_row(raw, _make_classified())
        assert row["source_refs"] == {"outscraper": "ChIJ123"}

    def test_tags_llm_contains_classified_tags(self) -> None:
        classified = _make_classified(tags=["pet_friendly", "parking"], confidence=0.88)
        row = _build_dim_row(_make_raw(), classified)
        assert row["tags_llm"]["tags"] == ["pet_friendly", "parking"]
        assert row["tags_llm"]["confidence"] == pytest.approx(0.88)


# ---------------------------------------------------------------------------
# _insert_dim_record
# ---------------------------------------------------------------------------

class TestInsertDimRecord:
    def test_inserts_to_correct_table(self) -> None:
        client = MagicMock()
        client.table.return_value.insert.return_value.execute.return_value.data = [{"id": "x"}]
        row = _build_dim_row(_make_raw(), _make_classified())
        count = _insert_dim_record(client, row)
        client.table.assert_called_with("dim_estabelecimentos")
        assert count == 1

    def test_returns_zero_when_no_data(self) -> None:
        client = MagicMock()
        client.table.return_value.insert.return_value.execute.return_value.data = []
        assert _insert_dim_record(client, {}) == 0


# ---------------------------------------------------------------------------
# _mark_processed
# ---------------------------------------------------------------------------

class TestMarkProcessed:
    def test_updates_correct_table(self) -> None:
        client = MagicMock()
        _mark_processed(client, ["id-1", "id-2"])
        client.table.assert_called_with("raw_crawled_data")

    def test_sets_processed_true(self) -> None:
        client = MagicMock()
        _mark_processed(client, ["id-1"])
        update_args = client.table.return_value.update.call_args[0][0]
        assert update_args == {"processed": True}

    def test_uses_in_filter_with_all_ids(self) -> None:
        client = MagicMock()
        _mark_processed(client, ["id-1", "id-2", "id-3"])
        in_args = client.table.return_value.update.return_value.in_.call_args
        assert in_args[0][0] == "id"
        assert set(in_args[0][1]) == {"id-1", "id-2", "id-3"}

    def test_does_nothing_for_empty_list(self) -> None:
        client = MagicMock()
        _mark_processed(client, [])
        client.table.assert_not_called()


# ---------------------------------------------------------------------------
# _log_pipeline
# ---------------------------------------------------------------------------

class TestLogPipeline:
    def test_writes_correct_pipeline_name(self) -> None:
        client = MagicMock()
        client.table.return_value.insert.return_value.execute.return_value.data = []
        with patch("src.transformers.llm_classifier.get_supabase_client", return_value=client):
            _log_pipeline(batch_id="b1", status="running")
        row = client.table.return_value.insert.call_args[0][0]
        assert row["pipeline_name"] == "llm_classifier"
        assert row["status"] == "running"

    def test_includes_duration_ms(self) -> None:
        client = MagicMock()
        client.table.return_value.insert.return_value.execute.return_value.data = []
        started = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        finished = datetime(2024, 1, 1, 10, 0, 2, tzinfo=timezone.utc)
        with patch("src.transformers.llm_classifier.get_supabase_client", return_value=client):
            _log_pipeline("b1", "success", started_at=started, finished_at=finished)
        row = client.table.return_value.insert.call_args[0][0]
        assert row["duration_ms"] == 2000

    def test_does_not_raise_on_supabase_error(self) -> None:
        client = MagicMock()
        client.table.side_effect = Exception("DB down")
        with patch("src.transformers.llm_classifier.get_supabase_client", return_value=client):
            _log_pipeline("b1", "running")  # must not raise


# ---------------------------------------------------------------------------
# run_classification (integration-style, all I/O mocked)
# ---------------------------------------------------------------------------

class TestRunClassification:
    def _patch_settings(self) -> MagicMock:
        mock = MagicMock()
        mock.OLLAMA_URL = "http://localhost:11434"
        mock.OLLAMA_MODEL = "llama3.1:8b"
        return mock

    def _make_full_client(
        self,
        raw_rows: list[dict] | None = None,
        rpc_data: str | None = None,
    ) -> MagicMock:
        """Build a mock Supabase client that handles all table() chains."""
        client = MagicMock()

        # SELECT from raw_crawled_data
        select_chain = (
            client.table.return_value
            .select.return_value
            .eq.return_value
            .limit.return_value
        )
        select_chain.execute.return_value.data = raw_rows or []

        # INSERT into dim_estabelecimentos
        client.table.return_value.insert.return_value.execute.return_value.data = [
            {"id": "dim-1"}
        ]

        # UPDATE (mark processed)
        client.table.return_value.update.return_value.in_.return_value.execute.return_value.data = []

        # INSERT into data_pipeline_logs
        # (reuses the same chain — MagicMock auto-returns)

        # RPC dedup check
        client.rpc.return_value.execute.return_value.data = rpc_data

        return client

    def test_returns_classified_count(self) -> None:
        raw_rows = [_make_raw_row("id-1"), _make_raw_row("id-2")]
        client = self._make_full_client(raw_rows=raw_rows, rpc_data=None)
        classified_json = _classified_json()

        with (
            patch("src.transformers.llm_classifier.get_supabase_client", return_value=client),
            patch("src.transformers.llm_classifier.get_settings", return_value=self._patch_settings()),
            patch("src.transformers.llm_classifier.ollama.Client") as MockOllama,
        ):
            MockOllama.return_value.chat.return_value = _make_ollama_response(classified_json)
            total = run_classification(batch_size=10)

        assert total == 2

    def test_returns_zero_when_no_records(self) -> None:
        client = self._make_full_client(raw_rows=[])

        with (
            patch("src.transformers.llm_classifier.get_supabase_client", return_value=client),
            patch("src.transformers.llm_classifier.get_settings", return_value=self._patch_settings()),
        ):
            total = run_classification()

        assert total == 0

    def test_skips_duplicate_records(self) -> None:
        raw_rows = [_make_raw_row("id-dup")]
        client = self._make_full_client(raw_rows=raw_rows, rpc_data="existing-dim-uuid")

        with (
            patch("src.transformers.llm_classifier.get_supabase_client", return_value=client),
            patch("src.transformers.llm_classifier.get_settings", return_value=self._patch_settings()),
            patch("src.transformers.llm_classifier.ollama.Client") as MockOllama,
        ):
            total = run_classification()
            MockOllama.return_value.chat.assert_not_called()

        assert total == 0

    def test_duplicate_records_still_marked_processed(self) -> None:
        raw_rows = [_make_raw_row("id-dup")]
        client = self._make_full_client(raw_rows=raw_rows, rpc_data="existing-dim-uuid")

        with (
            patch("src.transformers.llm_classifier.get_supabase_client", return_value=client),
            patch("src.transformers.llm_classifier.get_settings", return_value=self._patch_settings()),
            patch("src.transformers.llm_classifier.ollama.Client"),
        ):
            run_classification()

        client.table.return_value.update.assert_called_with({"processed": True})

    def test_skips_records_without_coordinates(self) -> None:
        payload = _make_raw(latitude=None, longitude=None).model_dump(mode="json")
        raw_rows = [_make_raw_row("id-nocoord", payload=payload)]
        client = self._make_full_client(raw_rows=raw_rows)

        with (
            patch("src.transformers.llm_classifier.get_supabase_client", return_value=client),
            patch("src.transformers.llm_classifier.get_settings", return_value=self._patch_settings()),
            patch("src.transformers.llm_classifier.ollama.Client") as MockOllama,
        ):
            total = run_classification()
            MockOllama.return_value.chat.assert_not_called()

        assert total == 0

    def test_llm_error_does_not_mark_record_processed(self) -> None:
        raw_rows = [_make_raw_row("id-llm-fail")]
        client = self._make_full_client(raw_rows=raw_rows, rpc_data=None)

        with (
            patch("src.transformers.llm_classifier.get_supabase_client", return_value=client),
            patch("src.transformers.llm_classifier.get_settings", return_value=self._patch_settings()),
            patch("src.transformers.llm_classifier.ollama.Client") as MockOllama,
            patch("time.sleep"),
        ):
            MockOllama.return_value.chat.side_effect = Exception("connection refused")
            total = run_classification()

        # Not marked as processed (in_ not called with this id)
        in_calls = client.table.return_value.update.return_value.in_.call_args_list
        processed_ids = [c[0][1] for c in in_calls] if in_calls else []
        flat_ids = [i for sublist in processed_ids for i in sublist]
        assert "id-llm-fail" not in flat_ids
        assert total == 0

    def test_invalid_payload_marked_processed_and_skipped(self) -> None:
        raw_rows = [_make_raw_row("id-bad", payload={"bad": "data"})]
        client = self._make_full_client(raw_rows=raw_rows)

        with (
            patch("src.transformers.llm_classifier.get_supabase_client", return_value=client),
            patch("src.transformers.llm_classifier.get_settings", return_value=self._patch_settings()),
            patch("src.transformers.llm_classifier.ollama.Client") as MockOllama,
        ):
            total = run_classification()
            MockOllama.return_value.chat.assert_not_called()

        assert total == 0
        client.table.return_value.update.assert_called_with({"processed": True})

    def test_logs_running_then_success(self) -> None:
        client = self._make_full_client(raw_rows=[])

        with (
            patch("src.transformers.llm_classifier.get_supabase_client", return_value=client),
            patch("src.transformers.llm_classifier.get_settings", return_value=self._patch_settings()),
        ):
            run_classification()

        log_rows = [
            c[0][0]
            for c in client.table.return_value.insert.call_args_list
            if isinstance(c[0][0], dict) and "pipeline_name" in c[0][0]
        ]
        statuses = [r["status"] for r in log_rows]
        assert "running" in statuses
        # No records → "skipped"
        assert "skipped" in statuses

    def test_logs_success_when_records_classified(self) -> None:
        raw_rows = [_make_raw_row("id-ok")]
        client = self._make_full_client(raw_rows=raw_rows)

        with (
            patch("src.transformers.llm_classifier.get_supabase_client", return_value=client),
            patch("src.transformers.llm_classifier.get_settings", return_value=self._patch_settings()),
            patch("src.transformers.llm_classifier.ollama.Client") as MockOllama,
        ):
            MockOllama.return_value.chat.return_value = _make_ollama_response(_classified_json())
            run_classification()

        log_rows = [
            c[0][0]
            for c in client.table.return_value.insert.call_args_list
            if isinstance(c[0][0], dict) and "status" in c[0][0]
        ]
        statuses = [r["status"] for r in log_rows]
        assert "success" in statuses

    def test_default_batch_size_is_used_when_not_specified(self) -> None:
        client = self._make_full_client(raw_rows=[])

        with (
            patch("src.transformers.llm_classifier.get_supabase_client", return_value=client),
            patch("src.transformers.llm_classifier.get_settings", return_value=self._patch_settings()),
        ):
            run_classification()

        limit_call = (
            client.table.return_value
            .select.return_value
            .eq.return_value
            .limit.call_args
        )
        assert limit_call[0][0] == _BATCH_SIZE
