import express from "express"
import { Env } from "./utils/config";
import { appRouter } from "./route";
import { health } from "./handler/health";
import { workers } from "./handler/workers";
const app = express();
app.use(express.json());
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", process.env.CORS_ORIGIN ?? "http://localhost:5173");
  res.header("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type,Authorization,token");

  if (req.method === "OPTIONS") {
    res.sendStatus(204);
    return;
  }

  next();
});

app.get("/health", health); // the status page's deep check (database, Redis, engine)
app.get("/health/workers", workers); // price-feeder, market-maker, db-poller, wss: fresh work, not just up
app.use("/api", appRouter);

app.listen(Env.PORT, () => {
  console.log(`backend started listening on port ${Env.PORT}`);
})
