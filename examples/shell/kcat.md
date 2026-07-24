# kcat one-liners

`kcat` (formerly kafkacat) is the fastest way to poke at a broker without
writing any code.

```sh
# What does the broker say it is?
kcat -b 127.0.0.1:9092 -L

# Produce three records
printf 'one\ntwo\nthree\n' | kcat -b 127.0.0.1:9092 -t demo -P

# Consume everything from the beginning and exit at the end
kcat -b 127.0.0.1:9092 -t demo -C -o beginning -e

# Consume as part of a group, so offsets are committed
kcat -b 127.0.0.1:9092 -t demo -G workers demo

# Produce with a key, and print keys when consuming
echo 'value' | kcat -b 127.0.0.1:9092 -t demo -P -k mykey
kcat -b 127.0.0.1:9092 -t demo -C -o beginning -e -f 'key=%k value=%s offset=%o\n'

# Binary payloads survive untouched. Send a file, read it back, compare.
kcat -b 127.0.0.1:9092 -t files -P -k logo.png < logo.png
kcat -b 127.0.0.1:9092 -t files -C -o beginning -c 1 -e > roundtrip.png
cmp logo.png roundtrip.png && echo identical
```

With SASL and TLS:

```sh
kcat -b broker.example.com:9092 -L \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanisms=SCRAM-SHA-256 \
  -X sasl.username=alice \
  -X sasl.password=...
```
