use csv::{ReaderBuilder, WriterBuilder};
use std::error::Error;
use url::{Url, ParseError};
use std::str;
use std::fs;
use std::path::Path;
use reqwest;

fn main() -> Result<(), Box<dyn Error>> {
    let output_file_name = "resource/crawler.csv";
    let parent_dir = "output";

    // Step 1: Create crawler.csv if it doesn't exist
    create_crawler_csv(output_file_name)?;

    // Step 2: Create output folders if they don't exist
    create_output_folders(output_file_name, parent_dir)?;

    Ok(())
}

fn create_crawler_csv(output_file_name: &str) -> Result<(), Box<dyn Error>> {
    // Skip creation if the CSV already exists
    if Path::new(output_file_name).exists() {
        println!("{} already exists. Skipping creation.", output_file_name);
        return Ok(());
    }

    // Open the input CSV file
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path("path/to/your/input_file.csv")?;

    // Create the output CSV file
    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .from_path(output_file_name)?;

    // Get the header record
    let headers = rdr.headers()?.clone();

    // Find the indices of the "WEBADDR" and "INSTNM" columns
    let webaddr_index = headers.iter().position(|h| h == "WEBADDR")
        .ok_or("WEBADDR column not found")?;
    let instnm_index = headers.iter().position(|h| h == "INSTNM")
        .ok_or("INSTNM column not found")?;

    // Write the headers to the output file
    wtr.write_record(&["WEBADDR", "INSTNM"])?;

    // Iterate over the records and process the URLs
    for result in rdr.byte_records() {
        match result {
            Ok(record) => {
                if let Some(raw_url) = record.get(webaddr_index) {
                    if let Ok(url) = str::from_utf8(raw_url) {
                        match ensure_https_scheme(url) {
                            Ok(full_url) => {
                                if let Some(instnm) = record.get(instnm_index) {
                                    let instnm_str = str::from_utf8(instnm).unwrap_or("Invalid UTF-8");
                                    wtr.write_record(&[full_url.as_str(), instnm_str])?;
                                }
                            }
                            Err(e) => println!("Error processing URL: {}", e),
                        }
                    } else {
                        println!("Error converting raw URL to UTF-8");
                    }
                }
            }
            Err(e) => println!("Error reading record: {}", e),
        }
    }

    Ok(())
}

fn create_output_folders(output_file_name: &str, parent_dir: &str) -> Result<(), Box<dyn Error>> {
    // Create the parent output directory if it doesn't exist
    if !Path::new(parent_dir).exists() {
        fs::create_dir(parent_dir)?;
    }

    // Open the crawler CSV file
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(output_file_name)?;

    // Get the header record
    let headers = rdr.headers()?.clone();

    // Find the indices of the "WEBADDR" and "INSTNM" columns
    let webaddr_index = headers.iter().position(|h| h == "WEBADDR")
        .ok_or("WEBADDR column not found")?;
    let instnm_index = headers.iter().position(|h| h == "INSTNM")
        .ok_or("INSTNM column not found")?;

    // Iterate over the records and process the URLs
    for result in rdr.byte_records() {
        match result {
            Ok(record) => {
                if let Some(raw_url) = record.get(webaddr_index) {
                    if let Ok(url) = str::from_utf8(raw_url) {
                        match ensure_https_scheme(url) {
                            Ok(full_url) => {
                                if let Some(instnm) = record.get(instnm_index) {
                                    let instnm_str = str::from_utf8(instnm).unwrap_or("Invalid UTF-8");

                                    // Create folder named after INSTNM inside the parent directory if it doesn't exist
                                    let folder_name = format!("{}/{}", parent_dir, instnm_str.trim());
                                    let html_output_path = format!("{}/index.html", folder_name);

                                    // Skip fetching if the folder and HTML output already exist
                                    if Path::new(&folder_name).exists() && Path::new(&html_output_path).exists() {
                                        println!("Skipping {} as it already exists with index.html.", folder_name);
                                        continue;
                                    }

                                    if !Path::new(&folder_name).exists() {
                                        fs::create_dir(&folder_name)?;
                                    }

                                    if let Ok(html_content) = fetch_html(&full_url) {
                                        fs::write(html_output_path, html_content)?;
                                    } else {
                                        println!("Skipping {} due to fetch error.", full_url);
                                    }
                                }
                            }
                            Err(e) => println!("Error processing URL: {}", e),
                        }
                    } else {
                        println!("Error converting raw URL to UTF-8");
                    }
                }
            }
            Err(e) => println!("Error reading record: {}", e),
        }
    }

    Ok(())
}

fn ensure_https_scheme(url: &str) -> Result<Url, ParseError> {
    let parsed_url = Url::parse(url);
    match parsed_url {
        Ok(url) => Ok(url),
        Err(_) => {
            // If parsing fails, assume the URL is missing the scheme and prepend "https://"
            Url::parse(&format!("https://{}", url))
        }
    }
}

fn fetch_html(url: &Url) -> Result<String, reqwest::Error> {
    let response = reqwest::blocking::get(url.as_str())?;
    let html = response.text()?;
    Ok(html)
}