Feature: gRPC client registration
  As a client
  I want to register through gRPC
  So that the server acknowledges my connection

  Scenario: Client can reply to queries from other clients
    Given a running gRPC server
    When a client registers with client id "clientQueryResponse" and the following query handlers:
      | users.GetAll  |
      | users.GetById |
      And a client registers with client id "clientQueryRequest"
      And the client "clientQueryRequest" sends an "users.GetAll" query
    Then the client "clientQueryRequest" receives a "ClientRegistered" message
      And the client "clientQueryResponse" receives a "ClientRegistered" message
      And the client "clientQueryResponse" receives a "QueryRequested" message
    When the client "clientQueryResponse" responds to the latest query from "clientQueryRequest"
    Then the client "clientQueryRequest" receives a "QueryResponded" message

  Scenario: Client request query that has no query handler registered
    Given a running gRPC server
    When a client registers with client id "clientQueryRequest"
      And the client "clientQueryRequest" sends an "users.GetAll" query
    Then the client "clientQueryRequest" receives a "ClientRegistered" message
      And the client "clientQueryRequest" receives a "QueryRequestedFailed" message
      | reason | QueryHandlerNotFound |

  Scenario: Client gets a request timed out for a QueryRequest
    Given a running gRPC server with the following env variables:
      | SERVER_QUERY_RESPONSE_TIMEOUT | 1ms |
    When a client registers with client id "clientQueryResponse" and the following query handlers:
      | users.GetAll  |
      | users.GetById |
      And a client registers with client id "clientQueryRequest"
      And the client "clientQueryRequest" sends an "users.GetAll" query
    Then the client "clientQueryRequest" receives a "ClientRegistered" message
      And the client "clientQueryResponse" receives a "ClientRegistered" message
      And the client "clientQueryResponse" receives a "QueryRequested" message
      And the client "clientQueryRequest" receives a "QueryRequestedFailed" message
      | reason | QueryTimedOut |

  Scenario: Client sends a duplicated query request id
    Given a running gRPC server
    When a client registers with client id "clientQueryResponse" and the following query handlers:
      | users.GetAll |
      And a client registers with client id "clientQueryRequest"
      And the client "clientQueryRequest" sends an "users.GetAll" query
      And the client "clientQueryRequest" sends the same "users.GetAll" query again
    Then the client "clientQueryRequest" receives a "ClientRegistered" message
      And the client "clientQueryResponse" receives a "ClientRegistered" message
      And the client "clientQueryResponse" receives a "QueryRequested" message
      And the client "clientQueryRequest" does not receives a "QueryRequestedError" message

  Scenario: Another client sends a duplicated query request id
    Given a running gRPC server
    When a client registers with client id "clientQueryResponse" and the following query handlers:
      | users.GetAll |
      And a client registers with client id "clientQueryRequestA"
      And a client registers with client id "clientQueryRequestB"
      And the client "clientQueryRequestA" sends an "users.GetAll" query
      And the client "clientQueryRequestB" sends an "users.GetAll" query with the same request id as "clientQueryRequestA"
    Then the client "clientQueryRequestA" receives a "ClientRegistered" message
      And the client "clientQueryRequestB" receives a "ClientRegistered" message
      And the client "clientQueryResponse" receives a "ClientRegistered" message
      And the client "clientQueryResponse" receives a "QueryRequested" message
      And the client "clientQueryRequestB" receives a "QueryRequestedFailed" message
      | reason | RequestIdDuplicated |
