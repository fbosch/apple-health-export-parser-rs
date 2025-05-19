use quick_xml::events::Event;
use quick_xml::reader::Reader;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};
use zip::ZipArchive;

#[derive(Debug, Serialize, Deserialize)]
struct HealthRecord {
    #[serde(rename = "type")]
    record_type: Option<String>,
    unit: Option<String>,
    value: Option<f64>,
    #[serde(rename = "startDate")]
    start_date: Option<String>,
    #[serde(rename = "endDate")]
    end_date: Option<String>,
}

fn read_export_xml(zip_path: &str) -> Result<String, Box<dyn std::error::Error>> {
    let file = File::open(zip_path)?;
    let mut archive = ZipArchive::new(BufReader::new(file))?;

    let mut export_file = archive
        .by_name("apple_health_export/export.xml")
        .map_err(|_| "Could not find 'export.xml' in the archive")?;

    let mut contents = String::new();
    export_file.read_to_string(&mut contents)?;

    Ok(contents)
}

fn parse_records(xml: &str, allowed_types: &HashSet<&str>) -> Vec<HealthRecord> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut records = Vec::new();
    let allow_all = allowed_types.is_empty();

    while let Ok(event) = reader.read_event_into(&mut buf) {
        if let Event::Empty(ref e) = event {
            if e.name().as_ref() == b"Record" {
                let mut record_type = None;

                // First pass: Only try to get the `type` attribute.
                for attr in e.attributes().flatten() {
                    if attr.key.as_ref() == b"type" {
                        record_type = Some(attr.unescape_value().unwrap().to_string());
                        break; // Found it, no need to keep looking.
                    }
                }

                if let Some(ref r_type) = record_type {
                    if allow_all || allowed_types.contains(r_type.as_str()) {
                        // Only parse remaining attributes if this record type is allowed.
                        let mut value = None;
                        let mut unit = None;
                        let mut start_date = None;
                        let mut end_date = None;

                        for attr in e.attributes().flatten() {
                            match attr.key.as_ref() {
                                b"value" => {
                                    let raw = attr.unescape_value().unwrap().to_string();
                                    value = raw.parse::<f64>().ok();
                                }
                                b"unit" => unit = Some(attr.unescape_value().unwrap().to_string()),
                                b"startDate" => {
                                    start_date = Some(attr.unescape_value().unwrap().to_string())
                                }
                                b"endDate" => {
                                    end_date = Some(attr.unescape_value().unwrap().to_string())
                                }
                                _ => {}
                            }
                        }

                        records.push(HealthRecord {
                            record_type: Some(r_type.clone()),
                            value,
                            unit,
                            start_date,
                            end_date,
                        });
                    }
                }
            }
        }

        if matches!(event, Event::Eof) {
            break;
        }

        buf.clear();
    }

    records
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let zip_path = "./export.zip";
    let output_path = "./output.json";
    let xml = read_export_xml(zip_path)?;

    let allowed_types: HashSet<&str> = ["HKQuantityTypeIdentifierRestingHeartRate"]
        .iter()
        .copied()
        .collect();

    let records = parse_records(&xml, &allowed_types);

    let json_output = serde_json::to_string_pretty(&records)?;

    fs::write(output_path, json_output)?;

    Ok(())
}
