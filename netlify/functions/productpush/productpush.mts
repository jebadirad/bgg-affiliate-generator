import type { Context } from "@netlify/functions";
import { main } from "./main.mjs";
export default async (req: Request, context: Context) => {
  main();
  return new Response("hello world");
};
