Feature: REST client details
  As an operator
  I want to inspect client last activity
  So that I can verify the server updates last_seen

  Scenario: last_seen is refreshed for /api/clients/{client_id}
    Given a running gRPC server with the following env variables:
      | SERVER_INTERVAL_HEARTBEAT_MESSAGE | 50ms |
      | SERVER_EXPIRATION_CLIENT          | 10ms |
    When a client registers with client id "myclient"
    Then the client "myclient" receives a "ClientRegistered" message
      And the client "myclient" last_seen in REST endpoint eventually decreases

