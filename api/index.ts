import { main } from "./productpush.js";
import { waitUntil } from "@vercel/functions";
import type { VercelRequest, VercelResponse } from "@vercel/node";
const productPush = async () => {
  await main();
};

export function GET(request: VercelRequest) {
  waitUntil(productPush());
  return new Response("hello world");
}
