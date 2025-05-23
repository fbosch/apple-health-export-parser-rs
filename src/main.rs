mod workout_activity;
use blake3;
use chrono::{Datelike, Duration, Utc};
use csv::Writer;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use smallstr::SmallString;
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::Instant;
use workout_activity::WorkoutActivityType;
use zip::ZipArchive;

#[derive(Debug, Serialize, Deserialize)]
struct HealthRecord {
    #[serde(rename = "type")]
    record_type: Option<SmallString<[u8; 32]>>,
    unit: Option<SmallString<[u8; 16]>>,
    value: Option<SmallString<[u8; 64]>>,
    #[serde(rename = "startDate")]
    start_date: Option<SmallString<[u8; 32]>>,
    #[serde(rename = "endDate")]
    end_date: Option<SmallString<[u8; 32]>>,
    metadata: HashMap<SmallString<[u8; 16]>, SmallString<[u8; 32]>>,
}

fn get_cache_dir() -> PathBuf {
    let mut temp_dir = env::temp_dir();
    temp_dir.push("apple_health_export_parser_rs");
    temp_dir
}

fn get_file_hash(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let data = fs::read(path)?;
    let hash = blake3::hash(&data);
    Ok(hash.to_hex().to_string())
}

fn is_in_last_12_months(date_str: &str) -> bool {
    if date_str.len() < 7 {
        return false;
    }
    let year: i32 = date_str[0..4].parse().unwrap_or(0);
    let month: u32 = date_str[5..7].parse().unwrap_or(0);

    let now = Utc::now();
    let cutoff_year = (now - Duration::days(365)).year();
    let cutoff_month = (now - Duration::days(365)).month();

    if year > cutoff_year {
        true
    } else if year == cutoff_year {
        month >= cutoff_month
    } else {
        false
    }
}

fn try_load_cache(cache_dir: &Path, hash: &str) -> Option<String> {
    let cache_path = cache_dir.join(format!("{}.xml", hash));
    if cache_path.exists() {
        fs::read_to_string(cache_path).ok()
    } else {
        None
    }
}

fn save_cache(cache_dir: &Path, hash: &str, data: &str) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(cache_dir)?;
    let cache_path = cache_dir.join(format!("{}.xml", hash));
    fs::write(cache_path, data)?;
    Ok(())
}

fn read_export_xml(zip_path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let cache_dir = get_cache_dir();
    let hash = get_file_hash(zip_path)?;

    if let Some(cached_xml) = try_load_cache(&cache_dir, &hash) {
        println!("Loaded XML from cache");
        return Ok(cached_xml);
    }

    let file = File::open(zip_path)?;
    let mut archive = ZipArchive::new(BufReader::new(file))?;
    let mut export_file = archive
        .by_name("apple_health_export/export.xml")
        .map_err(|_| "Could not find 'export.xml' in the archive")?;

    let mut contents = String::new();
    export_file.read_to_string(&mut contents)?;

    save_cache(&cache_dir, &hash, &contents)?;

    Ok(contents)
}

fn parse_records(xml: &str, allowed_types: &HashSet<&str>) -> Vec<HealthRecord> {
    let allow_all = allowed_types.is_empty();
    let chunks: Vec<&str> = xml.split("<Record ").collect();
    let metadata_keys_to_include: HashSet<&str> =
        ["HKActivityType", "HKPhysicalEffortEstimationType"]
            .iter()
            .copied()
            .collect();

    chunks
        .par_iter()
        .skip(1)
        .map(|chunk| {
            let full_chunk = format!("<Record {}", chunk);
            let mut reader = Reader::from_str(&full_chunk);
            reader.config_mut().trim_text(true);

            let mut buf = Vec::with_capacity(2048);

            let mut record_type = None;
            let mut value = None;
            let mut unit = None;
            let mut start_date = None;
            let mut end_date = None;

            let mut metadata: HashMap<SmallString<[u8; 16]>, SmallString<[u8; 32]>> =
                HashMap::new();

            let mut should_parse = allow_all;

            while let Ok(event) = reader.read_event_into(&mut buf) {
                match event {
                    Event::Empty(ref e) | Event::Start(ref e) => {
                        if e.name().as_ref() == b"Record" {
                            for attr in e.attributes().flatten() {
                                let key = attr.key.as_ref();
                                let value_ref = attr.value.as_ref();

                                if key == b"type" {
                                    if let Ok(v_str) = std::str::from_utf8(value_ref) {
                                        record_type = Some(SmallString::from(v_str));
                                        should_parse = allow_all || allowed_types.contains(v_str);
                                        if !should_parse {
                                            break;
                                        }
                                    }
                                    continue;
                                }

                                if !should_parse {
                                    continue;
                                }

                                if key == b"startDate" {
                                    if let Ok(v_str) = std::str::from_utf8(value_ref) {
                                        if !is_in_last_12_months(v_str) {
                                            should_parse = false;
                                            continue;
                                        }
                                        start_date = Some(SmallString::from(v_str));
                                    }
                                }

                                match key {
                                    b"value" => {
                                        if let Ok(v_str) = std::str::from_utf8(value_ref) {
                                            value = Some(SmallString::from(v_str));
                                        }
                                    }
                                    b"unit" => {
                                        if let Ok(v_str) = std::str::from_utf8(value_ref) {
                                            unit = Some(SmallString::from(v_str));
                                        }
                                    }
                                    b"startDate" => {
                                        if let Ok(v_str) = std::str::from_utf8(value_ref) {
                                            start_date = Some(SmallString::from(v_str));
                                        }
                                    }
                                    b"endDate" => {
                                        if let Ok(v_str) = std::str::from_utf8(value_ref) {
                                            end_date = Some(SmallString::from(v_str));
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        } else if e.name().as_ref() == b"MetadataEntry" && should_parse {
                            let mut key_opt: Option<SmallString<[u8; 16]>> = None;
                            let mut value_opt: Option<SmallString<[u8; 32]>> = None;

                            for attr in e.attributes().flatten() {
                                match attr.key.as_ref() {
                                    b"key" => {
                                        let key_str =
                                            std::str::from_utf8(attr.value.as_ref()).unwrap();
                                        key_opt = Some(SmallString::from(key_str));
                                    }
                                    b"value" => {
                                        let val_str =
                                            std::str::from_utf8(attr.value.as_ref()).unwrap();
                                        value_opt = Some(SmallString::from(val_str));
                                    }
                                    _ => {}
                                }
                            }

                            if let (Some(key), Some(mut value)) = (key_opt, value_opt) {
                                if key.as_str() == "HKActivityType" {
                                    if let Ok(code) = value.parse::<u32>() {
                                        let activity = WorkoutActivityType::from_u32(code);
                                        value = SmallString::from(activity.to_string());
                                    }
                                }
                                if metadata_keys_to_include.contains(key.as_str()) {
                                    metadata.insert(key, value);
                                }
                            }
                        }
                    }
                    Event::End(ref e) if e.name().as_ref() == b"Record" => {
                        break;
                    }
                    Event::Eof => break,
                    _ => {}
                }

                buf.clear();
            }

            if should_parse {
                Some(HealthRecord {
                    record_type,
                    value,
                    unit,
                    start_date,
                    end_date,
                    metadata,
                })
            } else {
                None
            }
        })
        .filter_map(|r| r)
        .collect()
}

fn write_csv(records: &[HealthRecord], path: &str) -> Result<(), Box<dyn Error>> {
    let mut wtr = Writer::from_path(path)?;

    wtr.write_record(&[
        "record_type",
        "value",
        "unit",
        "start_date",
        "end_date",
        "metadata",
    ])?;

    for rec in records {
        let meta_str = serde_json::to_string(&rec.metadata).unwrap_or_default();

        wtr.write_record(&[
            rec.record_type.as_deref().unwrap_or(""),
            rec.value.as_deref().unwrap_or(""),
            rec.unit.as_deref().unwrap_or(""),
            rec.start_date.as_deref().unwrap_or(""),
            rec.end_date.as_deref().unwrap_or(""),
            &meta_str,
        ])?;
    }

    wtr.flush()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let zip_path = "./export.zip";

    let allowed_types: HashSet<&str> = [
        "HKQuantityTypeIdentifierHeartRate",
        "HKCategoryTypeIdentifierHighHeartRateEvent",
        "HKQuantityTypeIdentifierRestingHeartRate",
        "HKQuantityTypeIdentifierPhysicalEffort",
        "HKQuantityTypeIdentifierBasalEnergyBurned",
        "HKQuantityTypeIdentifierActiveEnergyBurned",
        "HKQuantityTypeIdentifierDistanceWalkingRunning",
        "HKQuantityTypeIdentifierWalkingSpeed",
        "HKQuantityTypeIdentifierAppleStandTime",
        "HKQuantityTypeIdentifierAppleExerciseTime",
        "HKQuantityTypeIdentifierWalkingStepLength",
        "HKQuantityTypeIdentifierStepCount",
        "HKQuantityTypeIdentifierFlightsClimbed",
        "HKCategoryTypeIdentifierSleepAnalysis",
        "HKQuantityTypeIdentifierBodyMass",
        "HKCategoryTypeIdentifierToothbrushingEvent",
        "HKQuantityTypeIdentifierSixMinuteWalkTestDistance",
        "HKQuantityTypeIdentifierDietaryCaffeine",
        "HKQuantityTypeIdentifierDietaryWater",
    ]
    .iter()
    .copied()
    .collect();

    let t_read = Instant::now();
    let xml = read_export_xml(std::path::Path::new(zip_path))?;
    println!("Reading XML took {:.2?}", t_read.elapsed());

    let t_parse = Instant::now();
    let records = parse_records(&xml, &allowed_types);
    println!("Parsing XML took {:.2?}", t_parse.elapsed());
    println!("Found {} records", records.len());

    let t_serialize = Instant::now();
    let json_output = serde_json::to_string_pretty(&records)?;
    fs::write("./output.json", json_output)?;
    println!("JSON Serialization took {:.2?}", t_serialize.elapsed());

    let t_csv = Instant::now();
    write_csv(&records, "output.csv")?;
    println!("CSV Serialization took {:.2?}", t_csv.elapsed());

    let duration = start.elapsed();
    println!("Done in {:?}", duration);

    Ok(())
}
