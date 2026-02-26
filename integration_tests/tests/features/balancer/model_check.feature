Feature: Model Check Feature
  As a load balancer administrator
  I want to validate that requests contain the correct model
  So that only compatible requests are forwarded to llama.cpp instances

  Background:
    Given llama.cpp server "llama-1" is running (has 1 slot)
    And agent "agent-1" is running (observes "llama-1")
    And agent monitors llama.cpp every 100 milliseconds
    And agent "agent-1" is registered
    And balancer is running with check model enabled

  Scenario: Request with valid model should succeed
    When request "req-1" is sent to "/v1/chat/completions" with:
      | method | POST |
      | body   | {"model": "llama3", "messages": [{"role": "user", "content": "hello"}]} |
    Then response code is 200

  Scenario: Request with missing model field should return 400
    When request "req-2" is sent to "/v1/chat/completions" with:
      | method | POST |
      | body   | {"messages": [{"role": "user", "content": "hello"}]} |
    Then response code is 400

  Scenario: Request with unsupported model should return 404
    Given agent is registered with model "mistral"
    When request "req-3" is sent to "/v1/chat/completions" with:
      | method | POST |
      | body   | {"model": "llama3", "messages": [{"role": "user", "content": "hello"}]} |
    Then response code is 404

  Scenario: Request with malformed JSON should return 400
    When request "req-4" is sent to "/v1/chat/completions" with:
      | method | POST |
      | body   | {"model": "llama3", "messages": [{"role": "user", "content": "hello"} |
    Then response code is 400

  Scenario: Request with non-JSON content type should bypass model check
    When request "req-5" is sent to "/v1/chat/completions" with:
      | method | POST |
      | header | Content-Type: text/plain |
      | body   | plain text content |
    Then response code is 200

  Scenario: Long payload with model at end should succeed
    When request "req-6" is sent to "/v1/chat/completions" with:
      | method | POST |
      | body   | {"messages": [{"role": "user", "content": "hello"}], "model": "llama3"} |
    Then response code is 200

  Scenario: Very large payload without early model match should fail appropriately
    When request "req-7" is sent to "/v1/chat/completions" with:
      | method | POST |
      | body   | {"large_data": "x".repeat(2100000), "model": "llama3"} |
    Then response code is 400

  Scenario: Request with model in single quotes should succeed
    When request "req-8" is sent to "/v1/chat/completions" with:
      | method | POST |
      | body   | {"model": 'llama3', "messages": [{"role": "user", "content": "hello"}]} |
    Then response code is 200

  Scenario: Request with model with whitespace formatting should succeed
    When request "req-9" is sent to "/v1/chat/completions" with:
      | method | POST |
      | body   | {"model" : "llama3", "messages": [{"role": "user", "content": "hello"}]} |
    Then response code is 200
