Feature: Client disconnection during query request
  As a server
  I want to handle client disconnections gracefully
  So that the system remains stable even when clients disconnect during queries

  Scenario: Client disconnects after sending query with no handler
    Given a running gRPC server
    When a client registers with client id "clientRequest"
      And the client "clientRequest" sends an "users.GetAll" query
      And the client "clientRequest" disconnects immediately
    Then the client "clientRequest" does not receive any more messages
      And the server handles it gracefully without errors

  Scenario: Client disconnects and server cannot send query handler not found response
    Given a running gRPC server
    When a client registers with client id "clientQueryResponse" and the following query handlers:
      | users.GetById |
      And a client registers with client id "clientRequest"
      And the client "clientRequest" sends an "users.GetAll" query
      And the client "clientRequest" disconnects immediately
    Then the client "clientRequest" does not receive a "QueryRequestedFailed" message
      And the server handles it gracefully without errors

  Scenario: Responder client disconnects before sending response
    Given a running gRPC server
    When a client registers with client id "clientResponse" and the following query handlers:
      | users.GetAll |
      And a client registers with client id "clientRequest"
      And the client "clientRequest" sends an "users.GetAll" query
      And the client "clientRequest" disconnects immediately
    Then the client "clientResponse" receives a "QueryRequested" message
      And the server handles it gracefully without errors
