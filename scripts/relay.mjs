// Local TCP relay for dev on machines whose firewall blocks outbound TCP
// from freshly compiled binaries. Go services dial 127.0.0.1; node (allowed
// outbound) forwards to Railway. Usage: node scripts/relay.mjs
import net from "node:net";

const routes = [
  { local: 15432, host: "hayabusa.proxy.rlwy.net", port: 34088, name: "postgres" },
  { local: 19000, host: "altaria.proxy.rlwy.net", port: 24581, name: "clickhouse" },
  { local: 16379, host: "altaria.proxy.rlwy.net", port: 56877, name: "redis" },
];

for (const r of routes) {
  net
    .createServer((client) => {
      const upstream = net.connect(r.port, r.host);
      client.pipe(upstream);
      upstream.pipe(client);
      const drop = () => {
        client.destroy();
        upstream.destroy();
      };
      client.on("error", drop);
      upstream.on("error", drop);
    })
    .listen(r.local, "127.0.0.1", () =>
      console.log(`${r.name}: 127.0.0.1:${r.local} -> ${r.host}:${r.port}`),
    );
}
