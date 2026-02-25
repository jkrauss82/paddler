use regex::bytes::Regex;

#[test]
fn test_model_regex_extracts_double_quoted_model() {
    let re = Regex::new(r#""model"\s*:\s*["']([^"']*)["']"#).unwrap();
    let json = r#"{"model": "llama3", "messages": [{"role": "user", "content": "hello"}]}"#.as_bytes();

    let caps = re.captures(json).unwrap();
    let model = caps.get(1).unwrap();
    assert_eq!(String::from_utf8_lossy(model.as_bytes()), "llama3");
}

#[test]
fn test_model_regex_extracts_single_quoted_model() {
    let re = Regex::new(r#""model"\s*:\s*["']([^"']*)["']"#).unwrap();
    let json = r#"{"model": 'llama3', "messages": [{"role": "user", "content": "hello"}]}"#.as_bytes();

    let caps = re.captures(json).unwrap();
    let model = caps.get(1).unwrap();
    assert_eq!(String::from_utf8_lossy(model.as_bytes()), "llama3");
}

#[test]
fn test_model_regex_extracts_model_with_whitespace() {
    let re = Regex::new(r#""model"\s*:\s*["']([^"']*)["']"#).unwrap();
    let json = r#"{"model" : "llama3", "messages": [{"role": "user", "content": "hello"}]}"#.as_bytes();

    let caps = re.captures(json).unwrap();
    let model = caps.get(1).unwrap();
    assert_eq!(String::from_utf8_lossy(model.as_bytes()), "llama3");
}

#[test]
fn test_model_regex_extracts_model_at_end_of_payload() {
    let re = Regex::new(r#""model"\s*:\s*["']([^"']*)["']"#).unwrap();
    let json = r#"{"messages": [{"role": "user", "content": "hello"}], "model": "llama3"}"#.as_bytes();

    let caps = re.captures(json).unwrap();
    let model = caps.get(1).unwrap();
    assert_eq!(String::from_utf8_lossy(model.as_bytes()), "llama3");
}

#[test]
fn test_model_regex_returns_none_for_missing_model() {
    let re = Regex::new(r#""model"\s*:\s*["']([^"']*)["']"#).unwrap();
    let json = r#"{"messages": [{"role": "user", "content": "hello"}]}"#.as_bytes();

    let caps = re.captures(json);
    assert!(caps.is_none());
}

#[test]
fn test_model_regex_returns_none_for_malformed_json() {
    let re = Regex::new(r#""model"\s*:\s*["']([^"']*)["']"#).unwrap();
    let json = r#"{"messages": [{"role": "user", "content": "hello"} |"#.as_bytes();

    let caps = re.captures(json);
    assert!(caps.is_none());
}

#[test]
fn test_model_regex_extracts_model_with_special_chars() {
    let re = Regex::new(r#""model"\s*:\s*["']([^"']*)["']"#).unwrap();
    let json = r#"{"model": "llama3-70b-instruct", "messages": []}"#.as_bytes();

    let caps = re.captures(json).unwrap();
    let model = caps.get(1).unwrap();
    assert_eq!(String::from_utf8_lossy(model.as_bytes()), "llama3-70b-instruct");
}

#[test]
fn test_model_regex_extracts_model_from_large_payload() {
    let re = Regex::new(r#""model"\s*:\s*["']([^"']*)["']"#).unwrap();
    let mut json = String::from(r#"{"large_data": "#);
    json.push_str(&"x".repeat(1000000));
    json.push_str(r#", "model": "llama3"}"#);

    let caps = re.captures(json.as_bytes()).unwrap();
    let model = caps.get(1).unwrap();
    assert_eq!(String::from_utf8_lossy(model.as_bytes()), "llama3");
}

#[test]
fn test_model_regex_extracts_model_with_newlines() {
    let re = Regex::new(r#""model"\s*:\s*["']([^"']*)["']"#).unwrap();
    let json = r#"{
        "model": "llama3",
        "messages": [
            {"role": "user", "content": "hello"}
        ]
    }"#.as_bytes();

    let caps = re.captures(json).unwrap();
    let model = caps.get(1).unwrap();
    assert_eq!(String::from_utf8_lossy(model.as_bytes()), "llama3");
}