Feature: Model Check Feature
  As a load balancer administrator
  I want to validate that requests contain the correct model
  So that only compatible requests are forwarded to llama.cpp instances

  Background:
    Given llamacpp server is running
    And agent is running
    And agent monitors llamacpp every 100 milliseconds
    And balancer is running with check model enabled

  Scenario: Request with valid model should succeed
    When request is sent to path "/v1/chat/completions" with:
      | method | POST |
      | body   | {"model": "llama3", "messages": [{"role": "user", "content": "hello"}]} |
    Then response code is 200

  Scenario: Request with missing model field should return 400
    When request is sent to path "/v1/chat/completions" with:
      | method | POST |
      | body   | {"messages": [{"role": "user", "content": "hello"}]} |
    Then response code is 400

  Scenario: Request with unsupported model should return 404
    Given agent is registered with model "mistral"
    When request is sent to path "/v1/chat/completions" with:
      | method | POST |
      | body   | {"model": "llama3", "messages": [{"role": "user", "content": "hello"}]} |
    Then response code is 404

  Scenario: Request with malformed JSON should return 400
    When request is sent to path "/v1/chat/completions" with:
      | method | POST |
      | body   | {"model": "llama3", "messages": [{"role": "user", "content": "hello"} |
    Then response code is 400

  Scenario: Request with non-JSON content type should bypass model check
    When request is sent to path "/v1/chat/completions" with:
      | method | POST |
      | header | Content-Type: text/plain |
      | body   | plain text content |
    Then response code is 200

  Scenario: Long payload with model at end should succeed
    When request is sent to path "/v1/chat/completions" with:
      | method | POST |
      | body   | {"messages": [{"role": "user", "content": "hello"}], "model": "llama3"} |
    Then response code is 200

  Scenario: Very large payload without early model match should fail appropriately
    When request is sent to path "/v1/chat/completions" with:
      | method | POST |
      | body   | {"large_data": "x".repeat(2100000), "model": "llama3"} |
    Then response code is 400

  Scenario: Request with model in single quotes should succeed
    When request is sent to path "/v1/chat/completions" with:
      | method | POST |
      | body   | {"model": 'llama3', "messages": [{"role": "user", "content": "hello"}]} |
    Then response code is 200

  Scenario: Request with model with whitespace formatting should succeed
    When request is sent to path "/v1/chat/completions" with:
      | method | POST |
      | body   | {"model" : "llama3", "messages": [{"role": "user", "content": "hello"}]} |
    Then response code is 200