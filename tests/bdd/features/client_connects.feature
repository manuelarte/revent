Feature: gRPC client registration
  As a client
  I want to register through gRPC
  So that the server acknowledges my connection

  Scenario: Client registers successfully
    Given a running gRPC server
    When a client registers with client id "client123" and the following query handlers:
      | handler1 |
      | handler2 |
    Then the client "client123" receives a "ClientRegistered" message

  Scenario: Client registration fails when client id is duplicated
    Given a running gRPC server
    When connection "firstConnection" registers with client id "duplicatedClient"
      And connection "secondConnection" registers with client id "duplicatedClient"
    Then the client "firstConnection" receives a "ClientRegistered" message
      And the client "secondConnection" receives a "ClientRegistrationError" message
