"""
Unit tests for the Overpass OSM extractor.

All external dependencies (httpx, Supabase, settings) are mocked so
these tests run fully offline without any real credentials or network.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.extractors.overpass_osm import (
    FLORIANOPOLIS_BBOX,
    OVERPASS_QUERIES,
    _build_overpass_query,
    _fetch_overpass,
    _insert_batch,
    _log_pipeline,
    _parse_element,
    run_extraction,
)
from src.models.estabelecimento import RawEstablishment


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_node(
    node_id: int = 1,
    lat: float = -27.5,
    lon: float = -48.5,
    tags: dict | None = None,
) -> dict:
    return {
        "type": "node",
        "id": node_id,
        "lat": lat,
        "lon": lon,
        "tags": tags or {"name": "Hotel Teste", "tourism": "hotel"},
    }


def _make_way(
    way_id: int = 2,
    center_lat: float = -27.5,
    center_lon: float = -48.5,
    tags: dict | None = None,
) -> dict:
    return {
        "type": "way",
        "id": way_id,
        "center": {"lat": center_lat, "lon": center_lon},
        "tags": tags or {"name": "Restaurante Teste", "amenity": "restaurant"},
    }


def _make_overpass_response(elements: list[dict]) -> dict:
    return {"version": 0.6, "elements": elements}


def _make_mock_http_response(status_code: int = 200, data: dict | None = None) -> MagicMock:
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.json.return_value = data or _make_overpass_response([])
    mock_resp.text = json.dumps(data or {})
    return mock_resp


def _make_record(**kwargs) -> RawEstablishment:
    defaults = dict(
        source="overpass",
        source_id="node/1",
        name="Hotel Teste",
        latitude=-27.5,
        longitude=-48.5,
        raw_type="tourism=hotel",
    )
    defaults.update(kwargs)
    return RawEstablishment(**defaults)


# ---------------------------------------------------------------------------
# _build_overpass_query
# ---------------------------------------------------------------------------

class TestBuildOverpassQuery:
    def test_contains_key_value_pair(self) -> None:
        q = _build_overpass_query("tourism", "hotel", FLORIANOPOLIS_BBOX)
        assert "[tourism=hotel]" in q

    def test_contains_bounding_box(self) -> None:
        bbox = (-27.85, -48.65, -27.38, -48.33)
        q = _build_overpass_query("amenity", "restaurant", bbox)
        assert "-27.85,-48.65,-27.38,-48.33" in q

    def test_requests_json_output(self) -> None:
        q = _build_overpass_query("amenity", "cafe", FLORIANOPOLIS_BBOX)
        assert "[out:json]" in q

    def test_queries_both_nodes_and_ways(self) -> None:
        q = _build_overpass_query("amenity", "bar", FLORIANOPOLIS_BBOX)
        assert "node[amenity=bar]" in q
        assert "way[amenity=bar]" in q

    def test_requests_center_and_tags(self) -> None:
        q = _build_overpass_query("tourism", "hostel", FLORIANOPOLIS_BBOX)
        assert "out center tags;" in q


# ---------------------------------------------------------------------------
# _fetch_overpass
# ---------------------------------------------------------------------------

class TestFetchOverpass:
    def test_returns_parsed_json_on_success(self) -> None:
        expected = _make_overpass_response([_make_node()])
        mock_resp = _make_mock_http_response(200, expected)

        with patch("httpx.post", return_value=mock_resp):
            result = _fetch_overpass("query", "https://overpass-api.de/api/interpreter")

        assert result == expected

    def test_posts_to_correct_url(self) -> None:
        url = "https://overpass-api.de/api/interpreter"
        mock_resp = _make_mock_http_response(200)

        with patch("httpx.post", return_value=mock_resp) as mock_post:
            _fetch_overpass("query text", url)

        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        assert call_kwargs[0][0] == url
        assert call_kwargs[1]["data"] == {"data": "query text"}

    def test_raises_connection_error_on_429(self) -> None:
        mock_resp = _make_mock_http_response(429)
        with (
            patch("httpx.post", return_value=mock_resp),
            patch("time.sleep"),
            pytest.raises(ConnectionError, match="rate limit"),
        ):
            _fetch_overpass("query", "https://overpass-api.de/api/interpreter")

    def test_raises_connection_error_on_500(self) -> None:
        mock_resp = _make_mock_http_response(500)
        with (
            patch("httpx.post", return_value=mock_resp),
            patch("time.sleep"),
            pytest.raises(ConnectionError, match="status 500"),
        ):
            _fetch_overpass("query", "https://overpass-api.de/api/interpreter")

    def test_wraps_httpx_timeout_as_connection_error(self) -> None:
        import httpx as _httpx
        with (
            patch("httpx.post", side_effect=_httpx.TimeoutException("timed out")),
            patch("time.sleep"),
            pytest.raises(ConnectionError, match="Overpass API request failed"),
        ):
            _fetch_overpass("query", "https://overpass-api.de/api/interpreter")

    def test_retries_on_connection_error(self) -> None:
        success_resp = _make_mock_http_response(200)
        with (
            patch(
                "httpx.post",
                side_effect=[
                    ConnectionError("transient"),
                    ConnectionError("transient"),
                    success_resp,
                ],
            ),
            patch("time.sleep"),
        ):
            # Third attempt should succeed — no exception raised
            result = _fetch_overpass("query", "https://overpass-api.de/api/interpreter")

        assert "elements" in result


# ---------------------------------------------------------------------------
# _parse_element
# ---------------------------------------------------------------------------

class TestParseElement:
    def test_parses_node_with_full_tags(self) -> None:
        node = _make_node(
            node_id=42,
            lat=-27.6,
            lon=-48.4,
            tags={
                "name": "Pousada do Mar",
                "tourism": "guest_house",
                "phone": "+55 48 3333-1111",
                "website": "https://pousadadomar.com.br",
                "opening_hours": "Mo-Su 08:00-22:00",
                "cuisine": "seafood",
                "addr:street": "Rua das Flores",
                "addr:housenumber": "123",
                "addr:city": "Florianópolis",
            },
        )
        record = _parse_element(node, "tourism", "guest_house")

        assert record is not None
        assert record.source == "overpass"
        assert record.source_id == "node/42"
        assert record.name == "Pousada do Mar"
        assert record.latitude == pytest.approx(-27.6)
        assert record.longitude == pytest.approx(-48.4)
        assert record.raw_type == "tourism=guest_house"
        assert record.phone == "+55 48 3333-1111"
        assert record.website == "https://pousadadomar.com.br"
        assert record.opening_hours == "Mo-Su 08:00-22:00"
        assert record.cuisine == "seafood"
        assert "Rua das Flores" in (record.address or "")

    def test_parses_way_using_center_coords(self) -> None:
        way = _make_way(way_id=99, center_lat=-27.7, center_lon=-48.55)
        record = _parse_element(way, "amenity", "restaurant")

        assert record is not None
        assert record.source_id == "way/99"
        assert record.latitude == pytest.approx(-27.7)
        assert record.longitude == pytest.approx(-48.55)
        assert record.raw_type == "amenity=restaurant"

    def test_returns_none_when_node_missing_lat(self) -> None:
        node = {"type": "node", "id": 1, "lon": -48.5, "tags": {}}
        assert _parse_element(node, "tourism", "hotel") is None

    def test_returns_none_when_way_missing_center(self) -> None:
        way = {"type": "way", "id": 2, "tags": {}}
        assert _parse_element(way, "amenity", "bar") is None

    def test_uses_contact_phone_fallback(self) -> None:
        node = _make_node(tags={"name": "Bar X", "contact:phone": "+55 48 9999-0000", "amenity": "bar"})
        record = _parse_element(node, "amenity", "bar")
        assert record is not None
        assert record.phone == "+55 48 9999-0000"

    def test_uses_contact_website_fallback(self) -> None:
        node = _make_node(tags={"name": "Cafe Y", "contact:website": "https://cafe.com", "amenity": "cafe"})
        record = _parse_element(node, "amenity", "cafe")
        assert record is not None
        assert record.website == "https://cafe.com"

    def test_non_standard_tags_go_to_extra_data(self) -> None:
        node = _make_node(
            tags={"name": "Hotel Z", "tourism": "hotel", "stars": "4", "wheelchair": "yes"}
        )
        record = _parse_element(node, "tourism", "hotel")
        assert record is not None
        assert record.extra_data is not None
        assert record.extra_data.get("stars") == "4"
        assert record.extra_data.get("wheelchair") == "yes"

    def test_extra_data_is_none_when_no_extra_tags(self) -> None:
        node = _make_node(tags={"name": "Hostel Mínimo", "tourism": "hostel"})
        record = _parse_element(node, "tourism", "hostel")
        assert record is not None
        assert record.extra_data is None

    def test_returns_none_for_validation_failure(self) -> None:
        # latitude out of valid range should trigger Pydantic validation error
        node = {"type": "node", "id": 1, "lat": 999.0, "lon": -48.5, "tags": {}}
        assert _parse_element(node, "tourism", "hotel") is None


# ---------------------------------------------------------------------------
# _insert_batch
# ---------------------------------------------------------------------------

class TestInsertBatch:
    def test_returns_zero_for_empty_list(self) -> None:
        assert _insert_batch([], "batch-1") == 0

    def test_inserts_to_raw_crawled_data_table(self) -> None:
        records = [_make_record()]
        mock_client = MagicMock()
        mock_client.table.return_value.upsert.return_value.execute.return_value.data = [
            {"id": "abc"}
        ]

        with patch("src.extractors.overpass_osm.get_supabase_client", return_value=mock_client):
            count = _insert_batch(records, "batch-1")

        mock_client.table.assert_called_with("raw_crawled_data")
        assert count == 1

    def test_maps_fields_to_db_columns(self) -> None:
        records = [_make_record(source_id="node/42", name="Hotel Mar")]
        mock_client = MagicMock()
        mock_client.table.return_value.upsert.return_value.execute.return_value.data = []

        with patch("src.extractors.overpass_osm.get_supabase_client", return_value=mock_client):
            _insert_batch(records, "batch-xyz")

        upsert_args = mock_client.table.return_value.upsert.call_args
        row = upsert_args[0][0][0]

        assert row["source"] == "overpass"
        assert row["source_id"] == "node/42"
        assert row["batch_id"] == "batch-xyz"
        assert isinstance(row["payload"], dict)
        assert row["payload"]["name"] == "Hotel Mar"

    def test_returns_zero_when_all_duplicates(self) -> None:
        records = [_make_record()]
        mock_client = MagicMock()
        mock_client.table.return_value.upsert.return_value.execute.return_value.data = []

        with patch("src.extractors.overpass_osm.get_supabase_client", return_value=mock_client):
            count = _insert_batch(records, "batch-1")

        assert count == 0


# ---------------------------------------------------------------------------
# _log_pipeline
# ---------------------------------------------------------------------------

class TestLogPipeline:
    def test_writes_running_status(self) -> None:
        mock_client = MagicMock()
        mock_client.table.return_value.insert.return_value.execute.return_value.data = []

        with patch("src.extractors.overpass_osm.get_supabase_client", return_value=mock_client):
            _log_pipeline(batch_id="b1", status="running")

        mock_client.table.assert_called_with("data_pipeline_logs")
        row = mock_client.table.return_value.insert.call_args[0][0]
        assert row["status"] == "running"
        assert row["pipeline_name"] == "overpass_osm_extractor"
        assert row["batch_id"] == "b1"

    def test_includes_duration_ms_when_both_timestamps_provided(self) -> None:
        mock_client = MagicMock()
        mock_client.table.return_value.insert.return_value.execute.return_value.data = []

        started = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
        finished = datetime(2024, 6, 1, 10, 0, 3, tzinfo=timezone.utc)  # 3 seconds

        with patch("src.extractors.overpass_osm.get_supabase_client", return_value=mock_client):
            _log_pipeline(
                batch_id="b1",
                status="success",
                started_at=started,
                finished_at=finished,
            )

        row = mock_client.table.return_value.insert.call_args[0][0]
        assert row["duration_ms"] == 3000

    def test_does_not_raise_on_supabase_error(self) -> None:
        mock_client = MagicMock()
        mock_client.table.side_effect = Exception("DB unavailable")

        with patch("src.extractors.overpass_osm.get_supabase_client", return_value=mock_client):
            # Must not raise
            _log_pipeline(batch_id="b1", status="running")

    def test_includes_error_message_on_failure_status(self) -> None:
        mock_client = MagicMock()
        mock_client.table.return_value.insert.return_value.execute.return_value.data = []

        with patch("src.extractors.overpass_osm.get_supabase_client", return_value=mock_client):
            _log_pipeline(batch_id="b1", status="error", error_message="API timed out")

        row = mock_client.table.return_value.insert.call_args[0][0]
        assert row["error_message"] == "API timed out"


# ---------------------------------------------------------------------------
# run_extraction (integration-style, all I/O mocked)
# ---------------------------------------------------------------------------

class TestRunExtraction:
    def _make_mock_client(self, inserted_count: int = 1) -> MagicMock:
        mock = MagicMock()
        mock.table.return_value.upsert.return_value.execute.return_value.data = (
            [{"id": str(i)} for i in range(inserted_count)]
        )
        mock.table.return_value.insert.return_value.execute.return_value.data = []
        return mock

    def _make_http_resp_with_node(self, node_id: int = 1) -> MagicMock:
        data = _make_overpass_response([_make_node(node_id=node_id)])
        return _make_mock_http_response(200, data)

    def test_returns_total_inserted_count(self) -> None:
        mock_client = self._make_mock_client(inserted_count=1)
        http_resp = self._make_http_resp_with_node()

        with (
            patch("httpx.post", return_value=http_resp),
            patch("src.extractors.overpass_osm.get_supabase_client", return_value=mock_client),
            patch("src.extractors.overpass_osm.get_settings") as mock_settings,
            patch("time.sleep"),
        ):
            mock_settings.return_value.OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"
            total = run_extraction()

        # 7 queries × 1 node each = 7 inserts
        assert total == 7

    def test_all_seven_categories_are_queried(self) -> None:
        mock_client = self._make_mock_client(inserted_count=0)
        http_resp = _make_mock_http_response(200)
        posted_queries: list[str] = []

        def capture_post(url: str, **kwargs: object) -> MagicMock:
            posted_queries.append(kwargs.get("data", {}).get("data", ""))  # type: ignore[union-attr]
            return http_resp

        with (
            patch("httpx.post", side_effect=capture_post),
            patch("src.extractors.overpass_osm.get_supabase_client", return_value=mock_client),
            patch("src.extractors.overpass_osm.get_settings") as mock_settings,
            patch("time.sleep"),
        ):
            mock_settings.return_value.OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"
            run_extraction()

        assert len(posted_queries) == len(OVERPASS_QUERIES)
        all_queries_joined = " ".join(posted_queries)
        for qcfg in OVERPASS_QUERIES:
            assert f"{qcfg['key']}={qcfg['value']}" in all_queries_joined

    def test_respects_cooldown_between_queries(self) -> None:
        mock_client = self._make_mock_client(inserted_count=0)
        http_resp = _make_mock_http_response(200)

        with (
            patch("httpx.post", return_value=http_resp),
            patch("src.extractors.overpass_osm.get_supabase_client", return_value=mock_client),
            patch("src.extractors.overpass_osm.get_settings") as mock_settings,
            patch("time.sleep") as mock_sleep,
        ):
            mock_settings.return_value.OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"
            run_extraction()

        # 7 queries → 6 sleeps (no sleep before first query)
        assert mock_sleep.call_count == len(OVERPASS_QUERIES) - 1

    def test_logs_running_then_success(self) -> None:
        mock_client = self._make_mock_client(inserted_count=0)
        http_resp = _make_mock_http_response(200)

        with (
            patch("httpx.post", return_value=http_resp),
            patch("src.extractors.overpass_osm.get_supabase_client", return_value=mock_client),
            patch("src.extractors.overpass_osm.get_settings") as mock_settings,
            patch("time.sleep"),
        ):
            mock_settings.return_value.OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"
            run_extraction()

        log_rows = [
            call[0][0]
            for call in mock_client.table.return_value.insert.call_args_list
        ]
        statuses = [r["status"] for r in log_rows]
        assert "running" in statuses
        assert "success" in statuses

    def test_logs_error_status_on_api_failure(self) -> None:
        import httpx as _httpx

        mock_client = MagicMock()
        mock_client.table.return_value.insert.return_value.execute.return_value.data = []

        with (
            patch("httpx.post", side_effect=_httpx.NetworkError("Overpass down")),
            patch("src.extractors.overpass_osm.get_supabase_client", return_value=mock_client),
            patch("src.extractors.overpass_osm.get_settings") as mock_settings,
            patch("time.sleep"),
            pytest.raises(ConnectionError),
        ):
            mock_settings.return_value.OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"
            run_extraction()

        log_rows = [call[0][0] for call in mock_client.table.return_value.insert.call_args_list]
        statuses = [r["status"] for r in log_rows]
        assert "error" in statuses

    def test_groups_all_records_under_single_batch_id(self) -> None:
        mock_client = self._make_mock_client(inserted_count=0)
        http_resp = self._make_http_resp_with_node()

        with (
            patch("httpx.post", return_value=http_resp),
            patch("src.extractors.overpass_osm.get_supabase_client", return_value=mock_client),
            patch("src.extractors.overpass_osm.get_settings") as mock_settings,
            patch("time.sleep"),
        ):
            mock_settings.return_value.OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"
            run_extraction()

        # Collect batch_ids from every upsert call
        batch_ids: set[str] = set()
        for call in mock_client.table.return_value.upsert.call_args_list:
            rows = call[0][0]
            for row in rows:
                batch_ids.add(row["batch_id"])

        assert len(batch_ids) == 1, "All records must share a single batch_id"

    def test_skips_elements_without_coordinates(self) -> None:
        no_coord_element = {"type": "node", "id": 5, "tags": {"name": "Ghost"}}
        data = _make_overpass_response([no_coord_element])
        http_resp = _make_mock_http_response(200, data)
        mock_client = self._make_mock_client(inserted_count=0)

        with (
            patch("httpx.post", return_value=http_resp),
            patch("src.extractors.overpass_osm.get_supabase_client", return_value=mock_client),
            patch("src.extractors.overpass_osm.get_settings") as mock_settings,
            patch("time.sleep"),
        ):
            mock_settings.return_value.OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"
            total = run_extraction()

        assert total == 0
