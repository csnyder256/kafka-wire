# Running kafka-wire on a hosting platform

kafka-wire needs three things. Every platform below is judged on whether it
provides them:

1. **A persistent writable disk.** The log lives on disk. A platform with an
   ephemeral filesystem loses your data on every deploy.
2. **A raw TCP port.** The Kafka protocol is not HTTP. A platform that only
   routes HTTP or terminates TLS at layer 7 cannot carry it.
3. **A correct advertised address.** Clients connect once to bootstrap, then
   throw that address away and reconnect to whatever the broker advertises.
   This is the single most common reason a broker "connects and then hangs".

## The short version

| Platform | Persistent disk | Raw TCP | Verdict |
|---|---|---|---|
| Fly.io | volumes | yes | **Good.** The closest thing to a default answer. |
| Hetzner / any VPS | yes | yes | **Good.** Cheapest per GB. Use the systemd unit. |
| Render | persistent disk | yes | **Good**, on paid instance types. |
| Railway | volumes | yes (TCP proxy) | **Good.** Where this broker originally ran. |
| Koyeb | persistent volumes | yes | **Workable.** Set the advertised port explicitly. |
| DigitalOcean Droplet | yes | yes | **Good.** Plain VM, systemd unit. |
| AWS EC2 / Lightsail | EBS | yes | **Good.** |
| Kubernetes (any) | PVC | yes | **Good.** See `deploy/kubernetes`. |
| Nomad | host volume | yes | **Good.** See `deploy/nomad`. |
| DigitalOcean App Platform | no | HTTP only | **No.** |
| Google Cloud Run | no | HTTP/2 only | **No.** |
| AWS App Runner | no | HTTP only | **No.** |
| Heroku | ephemeral | HTTP only | **No.** |
| Vercel / Netlify / Cloudflare Workers | no | no | **No.** Not that kind of platform. |

A "No" here is not a criticism of the platform. Those products are built for
stateless HTTP services, and a message broker is the opposite of that.

## Fly.io

```sh
fly launch --no-deploy --name my-kafka-wire
fly volumes create kw_data --size 20 --region ord
```

`fly.toml`:

```toml
app = "my-kafka-wire"

[build]
  image = "ghcr.io/csnyder256/kafka-wire:latest"

[env]
  KAFKA_WIRE_STORAGE_DATADIR = "/data"
  KAFKA_WIRE_LISTENERS_KAFKA = "0.0.0.0:9092"
  KAFKA_WIRE_LISTENERS_ADMIN = "0.0.0.0:8080"
  # Reachable from other apps on your private network. Use the .internal name
  # rather than a public hostname unless you have turned on authentication.
  KAFKA_WIRE_LISTENERS_ADVERTISEDHOST = "my-kafka-wire.internal"
  KAFKA_WIRE_AUTH_ALLOWANON = "true"

[mounts]
  source = "kw_data"
  destination = "/data"

[[services]]
  internal_port = 9092
  protocol = "tcp"
  auto_stop_machines = false   # a broker that sleeps loses producers
```

Then `fly deploy`. Keep `auto_stop_machines` off: a scale-to-zero broker drops
connections and looks like a network fault to every client.

## Railway

Add a Volume mounted at `/data`, then set:

```
KAFKA_WIRE_STORAGE_DATADIR=/data
KAFKA_WIRE_LISTENERS_KAFKA=0.0.0.0:9092
KAFKA_WIRE_LISTENERS_ADVERTISEDHOST=<service>.railway.internal
KAFKA_WIRE_AUTH_ALLOWANON=true
```

Reach it from other services in the same project over the private network. To
reach it from outside, enable TCP proxying and set
`KAFKA_WIRE_LISTENERS_ADVERTISEDHOST` and `KAFKA_WIRE_LISTENERS_ADVERTISEDPORT`
to the proxy's hostname and port, **and turn on authentication first**.

## Render

Use a Private Service with a Disk mounted at `/data`. Private Services get a
raw TCP address on the internal network. A Web Service will not work: it only
routes HTTP.

## Koyeb

Koyeb maps the container port to a different published port, so the advertised
port has to be set explicitly:

```
KAFKA_WIRE_LISTENERS_ADVERTISEDPORT=<published port>
```

Leaving it derived from the listen address is the failure mode described at the
top of this file.

## A plain VM (Hetzner, DigitalOcean, EC2, Lightsail, or a machine under a desk)

The cheapest and most predictable option.

```sh
curl -sSL https://github.com/csnyder256/kafka-wire/releases/latest/download/kafka-wire_linux_amd64.tar.gz | tar xz
sudo mv kafka-wire /usr/local/bin/
sudo cp deploy/systemd/kafka-wire.service /etc/systemd/system/
sudo systemctl enable --now kafka-wire
```

Bind to a private interface, or turn on SASL and TLS before opening port 9092
to the internet. The broker refuses to start on a public address without
authentication unless you set `auth.allowanon`, which exists so that choice is
deliberate rather than accidental.

## Why Cloud Run and App Platform cannot work

Cloud Run gives each revision a fresh, empty, in-memory filesystem and routes
only HTTP. A Kafka broker there would accept writes, report success, and lose
everything on the next revision. Mounting Cloud Storage with a FUSE layer does
not fix it: the log requires ordinary file semantics, including durable
appends and renames, that an object-store gateway does not provide.

If you are on a platform in the "No" list and want managed Kafka semantics
anyway, run kafka-wire on a small VM alongside it, or use a hosted Kafka
service. Both are better answers than fighting the platform.
