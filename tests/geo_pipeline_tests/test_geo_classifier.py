import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from pipeline.geo_pipeline.geo_classifier import DataTypeDeterminer
from db.schema.geo_metadata_schema import GeoSeriesMetadata, GeoSampleMetadata


# ---------------- Test Fixtures ----------------

@pytest.fixture
def mock_session():
    """
    Provide a mock SQLAlchemy session for testing.
    """
    return MagicMock(spec=Session)


@pytest.fixture
def determiner():
    """
    Provide a DataTypeDeterminer instance for testing.
    """
    return DataTypeDeterminer(geo_id="GSE123456")


# ---------------- Test Cases ----------------

def test_initialization():
    """
    Test the initialization of the DataTypeDeterminer.
    """
    determiner = DataTypeDeterminer(geo_id="GSE123456")
    assert determiner.geo_id == "GSE123456"
    assert "GSE103322" in determiner.manual_single_cell_datasets


def test_process(mock_session, determiner):
    """
    Test the process method.
    """
    # Mock behavior of internal methods
    mock_samples = [GeoSampleMetadata(SampleID="GSM123456", LibraryStrategy="RNA-Seq")]

    with patch.object(determiner, '_get_series_metadata', return_value=GeoSeriesMetadata(Summary="This is a test")):
        with patch.object(determiner, '_get_samples', return_value=mock_samples):
            with patch.object(determiner, '_update_series_metadata') as mock_update:
                determiner.process()
                # Ensure update_series_metadata is called with resolved data types
                mock_update.assert_called_once()


def test_get_series_metadata(mock_session, determiner):
    """
    Test the _get_series_metadata method.
    """
    # Mock query return value
    mock_series = GeoSeriesMetadata(SeriesID="GSE123456", Summary="This is a test")
    mock_session.query.return_value.filter_by.return_value.one_or_none.return_value = mock_series

    # Call the method
    result = determiner._get_series_metadata(mock_session)

    # Validate the result
    assert result == mock_series


def test_get_samples(mock_session, determiner):
    """
    Test the _get_samples method.
    """
    # Mock query return value
    mock_sample = GeoSampleMetadata(SampleID="GSM123456", LibraryStrategy="RNA-Seq")
    mock_session.query.return_value.filter_by.return_value.all.return_value = [mock_sample]

    # Call the method
    samples = determiner._get_samples(mock_session)

    # Validate the results
    assert len(samples) == 1
    assert samples[0].SampleID == "GSM123456"


def test_classify_sample(determiner):
    """
    Test the _classify_sample method.
    """
    # Test classification logic
    assert determiner._classify_sample("spatial transcriptomics", None, None, None) == "Spatial Transcriptomics"
    assert determiner._classify_sample(None, "RNA-Seq", None, None) == "RNA-Seq"
    assert determiner._classify_sample(None, None, None, None) == "Microarray"


def test_handle_super_series(mock_session, determiner):
    """
    Test the _handle_super_series method.
    """
    # Mock super-series and sub-series metadata
    mock_series = GeoSeriesMetadata(RelatedDatasets='[{"target": "GSE654321", "type": "SuperSeries of"}]')
    mock_sub_series = GeoSeriesMetadata(DataTypes='["RNA-Seq"]')

    # Mock query behavior
    mock_session.query.return_value.filter_by.return_value.one_or_none.side_effect = [mock_series, mock_sub_series]

    # Call the method
    data_types = determiner._handle_super_series(mock_session)

    # Validate the aggregated data types
    assert data_types == {"RNA-Seq"}


def test_resolve_conflicts(determiner):
    """
    Test the _resolve_conflicts method.
    """
    # Test conflict resolution logic
    assert determiner._resolve_conflicts({"RNA-Seq", "Single Cell RNA-Seq"}) == ["Single Cell RNA-Seq"]
    assert set(determiner._resolve_conflicts({"RNA-Seq", "ATAC-Seq"})) == {"RNA-Seq", "ATAC-Seq"}


def test_update_series_metadata(mock_session, determiner):
    """
    Test the _update_series_metadata method.
    """
    # Mock series metadata
    mock_series = GeoSeriesMetadata(SeriesID="GSE123456", DataTypes=None)

    # Mock session query behavior
    mock_session.query.return_value.filter_by.return_value.one_or_none.return_value = mock_series

    # Call the method
    determiner._update_series_metadata(mock_session, ["RNA-Seq", "ChIP-Seq"])

    # Validate that the DataTypes field was updated
    assert mock_series.DataTypes == '["RNA-Seq", "ChIP-Seq"]'


def test_update_series_metadata_not_found(mock_session, determiner):
    """
    Test the _update_series_metadata method when the series is not found.
    """
    # Mock session query behavior
    mock_session.query.return_value.filter_by.return_value.one_or_none.return_value = None

    # Call the method
    determiner._update_series_metadata(mock_session, ["RNA-Seq"])

    # Ensure warning was logged
    with patch.object(determiner.logger, 'warning') as mock_warning:
        determiner._update_series_metadata(mock_session, ["RNA-Seq"])
        mock_warning.assert_called_with("Series GSE123456 not found in GeoSeriesMetadata.")
