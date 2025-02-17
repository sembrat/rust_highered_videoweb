use csv::{ReaderBuilder, WriterBuilder};
use std::error::Error;
use url::{Url, ParseError};
use std::str;
use std::fs;
use std::path::Path;
use reqwest;
use regex::Regex;
use scraper::{Html, Selector};

fn main() -> Result<(), Box<dyn Error>> {
    let output_file_name = "resource/crawler.csv";
    let parent_dir = "output";

    // Step 1: Create crawler.csv if it doesn't exist
    create_crawler_csv(output_file_name)?;

    // Step 2: Create output folders if they don't exist
    create_output_folders(output_file_name, parent_dir)?;

    // Step 3: Process videos in HTML files
    process_videos_in_html(parent_dir)?;

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
        .from_path("resource/hd2023.csv")?;

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
                        assert_eq!(url.is_empty(), continue);
                        println!("Processing: {}", url);
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
                                    let sanitized_instnm = sanitize_folder_name(instnm_str.trim());

                                    // Create folder named after INSTNM inside the parent directory if it doesn't exist
                                    let folder_name = format!("{}/{}", parent_dir, sanitized_instnm);
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

fn process_videos_in_html(parent_dir: &str) -> Result<(), Box<dyn Error>> {
    // Iterate through each subdirectory in the parent directory
    for entry in fs::read_dir(parent_dir)? {
        let entry = entry?;
        let subdir_path = entry.path();
        if subdir_path.is_dir() {
            let html_file_path = subdir_path.join("index.html");
            if html_file_path.exists() {
                let video_elements = extract_video_elements(&html_file_path)?;
                save_video_elements(&video_elements, &subdir_path)?;
            }
        }
    }
    Ok(())
}

fn extract_video_elements(html_file_path: &Path) -> Result<Vec<String>, Box<dyn Error>> {
    // Read the HTML file
    let html_content = fs::read_to_string(html_file_path)?;
    let document = Html::parse_document(&html_content);

    // Find all video elements
    let video_selector = Selector::parse("video, iframe").unwrap();
    let video_elements = document.select(&video_selector);

    // Extract video elements
    let mut video_elements_html = Vec::new();
    for video in video_elements {
        video_elements_html.push(video.html());
    }

    Ok(video_elements_html)
}

fn save_video_elements(video_elements: &[String], output_dir: &Path) -> Result<(), Box<dyn Error>> {
    for (i, element) in video_elements.iter().enumerate() {
        // Define the output file path
        let output_file_path = output_dir.join(format!("video_{}.html", i + 1));

        // Skip creation if the file already exists
        if output_file_path.exists() {
            println!("{} already exists. Skipping creation.", output_file_path.display());
            continue;
        }

        // Save the video element to a new HTML file
        fs::write(output_file_path, element)?;
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

fn sanitize_folder_name(name: &str) -> String {
    let re = Regex::new(r"[^\w\s-]").unwrap();
    let sanitized_name = re.replace_all(name, "").to_string();
    sanitized_name.replace(" ", "_")
}