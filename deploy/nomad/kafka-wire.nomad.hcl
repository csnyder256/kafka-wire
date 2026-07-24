job "kafka-wire" {
  datacenters = ["dc1"]
  type        = "service"

  group "broker" {
    count = 1

    # A host volume keeps the log across restarts. Declare it on the client:
    #   client { host_volume "kafka-wire" { path = "/opt/kafka-wire" } }
    volume "data" {
      type      = "host"
      source    = "kafka-wire"
      read_only = false
    }

    network {
      port "kafka" { to = 9092 }
      port "admin" { to = 8080 }
    }

    service {
      name = "kafka-wire"
      port = "kafka"
      check {
        type     = "http"
        port     = "admin"
        path     = "/health"
        interval = "15s"
        timeout  = "3s"
      }
    }

    task "broker" {
      driver = "docker"
      kill_timeout = "40s"   # must exceed shutdown.grace

      volume_mount {
        volume      = "data"
        destination = "/data"
      }

      config {
        image = "ghcr.io/csnyder256/kafka-wire:latest"
        ports = ["kafka", "admin"]
      }

      env {
        KAFKA_WIRE_STORAGE_DATADIR = "/data"
        KAFKA_WIRE_LISTENERS_KAFKA = "0.0.0.0:9092"
        KAFKA_WIRE_LISTENERS_ADMIN = "0.0.0.0:8080"
        # Nomad maps a random host port to 9092, so the advertised port must
        # be the HOST port or clients will redirect themselves into nothing.
        KAFKA_WIRE_LISTENERS_ADVERTISEDHOST = "${attr.unique.network.ip-address}"
        KAFKA_WIRE_LISTENERS_ADVERTISEDPORT = "${NOMAD_HOST_PORT_kafka}"
        KAFKA_WIRE_AUTH_ALLOWANON  = "true"
      }

      resources {
        cpu    = 500
        memory = 1024
      }
    }
  }
}
