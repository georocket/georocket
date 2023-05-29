use std::future::Future;
use tokio::sync::mpsc;

/// The `GeoJsonParser` incrementally parses a GeoJSON file and returns Strings containing `Feature`s
/// or `Geometry`s, along with metadata associated with the `Feature` or `Geometry`.
/// Processed features can be retrieved from the the `GeoJsonParser` as they become available.
pub struct GeoJsonParser {
    processed_features: mpsc::Receiver<ProcessedGeoJsonFeature>,
    //TODO: Add appropriate fields
}

/// The `Splitter` is responsible for constructing Strings out of the GeoJSON `Feature`s objects contained
/// int the `"features"` field of GeoJSON `FeatureCollection` objects.
/// It passes the strings to the `FeatureProcessor` for processing.
struct Splitter<R> {
    reader: R,
    channel: mpsc::Sender<String>,
}

/// The `FeatureProcessor` receives GeoJSON `Feature`s from a channel. It processes the
/// `Feature`s into `ProcessedGeoJsonFeature`s, which can be retrieved from the associated
/// `mpsc::Receiver<_>` channel.
struct FeatureProcessor<F> {
    geo_json_string_receive_channel: mpsc::Receiver<String>,
    processed_feature_send_channel: mpsc::Sender<ProcessedGeoJsonFeature>,
    processing_function: F,
}

impl<F, Fut> FeatureProcessor<F>
where
    F: Fn(mpsc::Sender<ProcessedGeoJsonFeature>, String) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'static,
{
    /// Construct a new `FeatureProcessor`, which takes GeoJSON features to process out of the provided
    /// `geo_json_string_in_channel` and returns `ProcessedGeoJsonFeature`s in the `processed_feature_out_channel`,
    fn new(
        geo_json_string_receive_channel: mpsc::Receiver<String>,
        processed_feature_send_channel: mpsc::Sender<ProcessedGeoJsonFeature>,
        processing_function: F,
    ) -> Self {
        Self {
            geo_json_string_receive_channel,
            processed_feature_send_channel,
            processing_function,
        }
    }

    /// Construct a new `FeatureProcessor` along with the required channels.
    /// Channels are created with a capacity as specified by `feature_channel_capacity` and
    /// `processed_feature_channel_capacity`
    fn new_with_channels(
        processing_function: F,
        feature_channel_capacity: usize,
        processed_feature_channel_capacity: usize,
    ) -> (
        Self,
        mpsc::Sender<String>,
        mpsc::Receiver<ProcessedGeoJsonFeature>,
    ) {
        let (feature_send, feature_receiver) = mpsc::channel(feature_channel_capacity);
        let (processed_feature_sender, processed_feature_receiver) =
            mpsc::channel(processed_feature_channel_capacity);
        (
            Self::new(
                feature_receiver,
                processed_feature_sender,
                processing_function,
            ),
            feature_send,
            processed_feature_receiver,
        )
    }

    /// Consumes the `FeatureProcessor` and begins processing GeoJSON features by applying the
    /// `processing_function` supplied in the constructor.
    /// Returns once the channel supplying the features has been closed and the final feature has
    /// been processed.
    async fn run(mut self) -> Result<(), ()> {
        while let Some(feature) = self.geo_json_string_receive_channel.recv().await {
            let out_channel = self.processed_feature_send_channel.clone();
            tokio::spawn((self.processing_function)(out_channel, feature));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct GeoJsonMetaDataCollection {
    inner: Vec<GeoJsonMetaData>,
}

impl GeoJsonMetaDataCollection {
    fn new() -> Self {
        Self { inner: Vec::new() }
    }
}

#[derive(Debug)]
pub enum GeoJsonMetaData {
    //TODO: Add variants such as `BoundingBox` or other meta data that is intended
    // for indexing or other post-processing requirements
}

/// Contains the GeoJSON string of the processed feature as well as metadata for further processing
/// or indexing
#[derive(Debug)]
pub struct ProcessedGeoJsonFeature {
    //The unmodified JSON string containing the object
    feature_json: String,
    //The metadata associated with the object
    meta_data: Option<GeoJsonMetaDataCollection>,
}

impl ProcessedGeoJsonFeature {
    /// Constructs a new `ProcessedGeoJsonFeature` from the `feature_json.
    /// No metadata is created.
    pub fn new_no_metadata(feature_json: String) -> Self {
        Self {
            feature_json,
            meta_data: None,
        }
    }
    /// Returns a reference to the contained GeoJSON feature string
    pub fn feature_json(&self) -> &str {
        self.feature_json.as_str()
    }
    /// Returns a reference to the contained metadata
    pub fn meta_data(&self) -> &Option<GeoJsonMetaDataCollection> {
        &self.meta_data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_feature_processor() {
        // some GeoJSON feature objects for testing
        let features: Vec<String> = vec![
            "{ \"type\": \"Feature\", \"geometry\": { \"type\": \"Point\", \"coordinates\": [102.0, 0.5] }, \"properties\": { \"prop0\": \"value0\" } }".into(),
            "{ \"type\": \"Feature\", \"geometry\": { \"type\": \"LineString\", \"coordinates\": [ [102.0, 0.0], [103.0, 1.0], [104.0, 0.0], [105.0, 1.0] ] }, \"properties\": { \"prop0\": \"value0\", \"prop1\": 0.0 } }".into(),
            "{ \"type\": \"Feature\", \"geometry\": { \"type\": \"Polygon\", \"coordinates\": [ [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ] ] } }".into(),
        ];

        // Input function for the `FeatureProcessor`
        // For testing purposes, no processing or formatting of the GeoJSON feature is done.
        // The `ProcessedGeoJsonFeature` sent out through the `out_channel` is directly constructed from
        // the `feature` and the `meta_data` field is initialised to `None`.
        async fn no_processing(
            out_channel: mpsc::Sender<ProcessedGeoJsonFeature>,
            feature: String,
        ) {
            let processed_feature = ProcessedGeoJsonFeature::new_no_metadata(feature);
            out_channel.send(processed_feature).await.unwrap();
        }

        let (processor, json_sender, mut processed_feature_receiver) =
            FeatureProcessor::new_with_channels(no_processing, 64, 64);

        // run the `FeatureProcessor`
        let processor_handle = tokio::spawn(processor.run());

        // send all the test features to the `FeatureProcessor`
        for feature in features.iter().cloned() {
            json_sender.send(feature).await.unwrap();
        }

        // drop the send end to signal that no more features will be sent though the channel
        drop(json_sender);

        // await the processing of the features and check for validity
        let mut processed_features = Vec::new();
        while let Some(processed_feature) = processed_feature_receiver.recv().await {
            assert!(features.contains(&processed_feature.feature_json));
            processed_features.push(processed_feature);
        }
        assert_eq!(processed_features.len(), features.len());
        // check that the FeatureProcessor has terminated properly
        assert!(processor_handle.await.unwrap().is_ok());
    }
}
