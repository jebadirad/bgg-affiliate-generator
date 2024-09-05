import type { Context } from "@netlify/functions";
import { main } from "../../src/productpush.js";
export default async (req: Request, context: Context) => {
  await main();
  return new Response("hello world");
};
