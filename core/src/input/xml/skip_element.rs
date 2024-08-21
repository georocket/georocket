use quick_xml::events::Event;

/// Specifies if an element should be skipped during import
pub trait ShouldBeSkipped {
    /// Returns `true` if `self` should be skipped during import
    fn should_be_skipped(&self) -> bool;
}

/// Specifies if an XML element should be skipped during import
impl ShouldBeSkipped for Event<'_> {
    fn should_be_skipped(&self) -> bool {
        match self {
            Event::Start(s) => {
                let local_name = s.local_name();
                local_name.as_ref() == b"Envelope"
            }
            _ => false,
        }
    }
}
