import "dotenv/config";
import neatCsv from "neat-csv";

import type { VercelRequest, VercelResponse } from "@vercel/node";

import * as fs from "node:fs";
import { put } from '@vercel/blob';

import * as path from "node:path";
import { createObjectCsvWriter } from "csv-writer";
import "@shopify/shopify-api/adapters/node";
import { shopifyApi, LATEST_API_VERSION } from "@shopify/shopify-api";
import { fileFromSync } from "node-fetch";
const shopify = shopifyApi({
  apiKey: process.env.api_key,
  apiSecretKey: process.env.api_secret as string,
  scopes: ["read_products", "write_products"],
  isEmbeddedApp: false,
  apiVersion: LATEST_API_VERSION,
  hostName: process.env.hostname as string,
});
const PATH_TO_MAIN = path.join(
  process.cwd(),
  "./files/export_boardgames_primary.csv"
);

const PATH_TO_RPG = path.join(
  process.cwd(),
  "./files/export_rpgitems_primary.csv"
);


type BGGUpload = {
  gameid: string;
  url: string;
  price: number;
  currency: "USD";
  enabled: 1 | 0;
  show_from: 1 | 0;
};

const affiliateUrlBuilder = ({ handle }: { handle: string }) => {
  return `${process.env.WEBSITE_URL}/${handle}?utm_source=boardgamegeek&utm_medium=referral&utm_campaign=buy_a_copy`;
};

const session = shopify.session.customAppSession(
  shopify.utils.sanitizeShop(process.env.shopifyurl as string) as string
);
session.accessToken = process.env.access_token;
const client = new shopify.clients.Graphql({
  session,
  apiVersion: LATEST_API_VERSION,
});

const getAllProducts = async ({
  after = "",
}: {
  after?: string;
}): Promise<
  {
    id: string;
    handle: string;
    metafield: string | null;
    totalInventory: number;
    price: number;
    barcode: string | null;
  }[]
> => {
  const queryString = `{
        products(first: 150, ${after ? `after: "${after}"` : ""} 
            query: "status:Active AND ((product_type:'Board Game') OR (product_type:'Board Games') OR (product_type:'Card Game') OR (product_type:'Dice Game') OR (product_type:'Non-Collectible Card Games') OR (tag:boardgame OR (tag:rpg AND -tag:rpg dice sets) OR tag:miniatures)) AND -tag:needs_bgg_manual"
        ){
            edges {
                node {
                    id
                    handle
                    totalInventory
                    priceRangeV2 { minVariantPrice { amount } }
                    metafield(key: "bgg_game_id", namespace: "custom"){ value }
                    variants(first: 1){ edges { node { barcode } } }
                }
            }
            pageInfo { hasNextPage endCursor }
        }
    }`;
  const {
    body: {
      data: { products },
    },
  } = await client.query<{
    data: {
      products: {
        edges: Array<{
          node: {
            id: string;
            handle: string;
            totalInventory: number;
            metafield: { value: string } | null;
            priceRangeV2: { minVariantPrice: { amount: number } };
            variants: { edges: Array<{ node: { barcode: string | null } }> };
          };
        }>;
        pageInfo: { hasNextPage: boolean; endCursor: string };
      };
    };
  }>({
    data: queryString,
  });

  const formattedProducts = products.edges.map(
    ({ node: { id, handle, metafield, priceRangeV2, totalInventory, variants } }) => {
      const raw = Number(priceRangeV2.minVariantPrice.amount);
      const rounded = Math.round((raw + Number.EPSILON) * 100) / 100;
      return {
        id,
        handle,
        totalInventory,
        metafield: metafield ? metafield.value : null,
        price: rounded,
        barcode: variants.edges[0]?.node.barcode || null,
      };
    }
  );
  if (products.pageInfo.hasNextPage) {
    const nextPage = await getAllProducts({ after: products.pageInfo.endCursor });
    return formattedProducts.concat(nextPage);
  }
  return formattedProducts;
};

import csvParser from 'csv-parser';

// Helper to normalize barcodes (digits only)
const normalizeBarcode = (val: string | null | undefined) => {
  if (!val) return null;
  const digits = val.replace(/[^0-9Xx]/g, '').toUpperCase();
  return digits.length ? digits : null;
};

// Load a two-column CSV (objectid,<idType>) into Map<identifier, Set<objectid>>
async function loadIdentifierIndex(filePath: string): Promise<Map<string, Set<string>>> {
  const map = new Map<string, Set<string>>();
  await new Promise<void>((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csvParser())
      .on('data', (row: any) => {
        const objectid = (row.objectid || '').toString().trim();
        const idValRaw = Object.keys(row).find(k => k !== 'objectid');
        if (!idValRaw) return;
        const idVal = normalizeBarcode(row[idValRaw]);
        if (!idVal || !objectid) return;
        if (!map.has(idVal)) map.set(idVal, new Set());
        map.get(idVal)!.add(objectid);
      })
      .on('end', () => resolve())
      .on('error', reject);
  });
  return map;
}

let upcIndex: Map<string, Set<string>> | null = null;
let gtinIndex: Map<string, Set<string>> | null = null;
let isbnIndex: Map<string, Set<string>> | null = null;

async function ensureIndexes() {
  if (upcIndex && gtinIndex && isbnIndex) return;
  const base = path.join(process.cwd(), 'files');
  // Filenames based on existing repo structure
  const UPC_PATH = path.join(base, 'export_boardgames_external_ids_upc.csv');
  const GTIN_PATH = path.join(base, 'export_boardgames_external_ids_gtin.csv');
  const ISBN_PATH = path.join(base, 'export_boardgames_external_ids_isbn.csv');
  [upcIndex, gtinIndex, isbnIndex] = await Promise.all([
    loadIdentifierIndex(UPC_PATH),
    loadIdentifierIndex(GTIN_PATH),
    loadIdentifierIndex(ISBN_PATH),
  ]);
}

// Shopify mutations
const PRODUCT_UPDATE_MUTATION = `mutation productUpdate($id: ID!, $metafields: [MetafieldInput!]!) {\n  productUpdate(product: { id: $id, metafields: $metafields }) {\n    userErrors { field message }\n  }\n}`;
const ADD_TAGS_MUTATION = `mutation tagsAdd($id: ID!, $tags: [String!]!) {\n  tagsAdd(id: $id, tags: $tags) { userErrors { field message } }\n}`;

async function setBGGMetafield(productId: string, objectid: string) {
  const dry = process.env.DRY_RUN === '1';
  if (dry) {
    console.log(`[DRY_RUN] Would set metafield bgg_game_id=${objectid} on ${productId}`);
    return;
  }
  const res = await client.query({
    data: {
      query: PRODUCT_UPDATE_MUTATION,
      variables: {
        id: productId,
        metafields: [
          {
            namespace: 'custom',
            key: 'bgg_game_id',
            type: 'single_line_text_field',
            value: objectid,
          },
        ],
      },
    },
  });
  const errors = (res.body as any)?.data?.productUpdate?.userErrors;
  if (errors && errors.length) {
    console.error('productUpdate userErrors', errors);
  }
  console.log('metafield set response', JSON.stringify(res.body));
}

async function tagProductManual(productId: string) {
  const dry = process.env.DRY_RUN === '1';
  if (dry) {
    console.log(`[DRY_RUN] Would tag product ${productId} needs_bgg_manual`);
    return;
  }
  const res = await client.query({
    data: {
      query: ADD_TAGS_MUTATION,
      variables: { id: productId, tags: ['needs_bgg_manual'] },
    },
  });
  console.log('tag add response', JSON.stringify(res.body));
}

// Attempt single unique match given barcode
function matchBarcode(barcode: string | null): string | null {
  if (!barcode) return null;
  const norm = normalizeBarcode(barcode);
  if (!norm) return null;
  // Priority: UPC > GTIN > ISBN
  const tryIndex = (idx: Map<string, Set<string>> | null) => {
    if (!idx) return null;
    const set = idx.get(norm);
    if (!set) return null;
    if (set.size === 1) return [...set][0];
    return null; // ambiguous -> treat as no unique match
  };
  return (
    tryIndex(upcIndex) ||
    tryIndex(gtinIndex) ||
    tryIndex(isbnIndex) ||
    null
  );
}

export async function main() {
  await ensureIndexes();
  const products = await getAllProducts({});
  const mutationConcurrency = Number(process.env.MUTATION_CONCURRENCY || 8); // adjust via env
  const queue: Promise<any>[] = [];
  const runWithConcurrency = async (fn: () => Promise<any>) => {
    if (queue.length >= mutationConcurrency) {
      await Promise.race(queue);
    }
    const p = fn().finally(() => {
      const idx = queue.indexOf(p);
      if (idx >= 0) queue.splice(idx, 1);
    });
    queue.push(p);
  };
  for (const p of products) {
    if (p.metafield) continue;
    const match = matchBarcode(p.barcode);
    if (match) {
      await runWithConcurrency(async () => {
        await setBGGMetafield(p.id, match);
        p.metafield = match;
      });
    } else {
      await runWithConcurrency(async () => {
        await tagProductManual(p.id);
      });
    }
  }
  // Wait remaining mutations
  await Promise.all(queue);
  // After matching, load primary/rpg ids once for validation
  const primaries = (await neatCsv(fs.createReadStream(PATH_TO_MAIN), {
    headers: ["objectid", "name"],
  })) as Array<{ objectid: string; name: string }>;
  const rpgs = (await neatCsv(fs.createReadStream(PATH_TO_RPG), {
    headers: ["objectid", "name"],
  })) as Array<{ objectid: string; name: string }>;
  const validIds = new Set<string>([...primaries.map(p=>p.objectid), ...rpgs.map(r=>r.objectid)]);

  const matchedProducts = products.filter(p => p.metafield && validIds.has(p.metafield));
  const failedProducts = products.filter(p => !p.metafield || !validIds.has(p.metafield));

  const formattedProducts: Array<BGGUpload> = matchedProducts.map(
    ({ metafield, price, handle, totalInventory }) => {
      return {
        currency: "USD",
        enabled: totalInventory > 0 ? 1 : 0,
        gameid: metafield as string,
        price,
        show_from: 1,
        url: affiliateUrlBuilder({ handle }),
      };
    }
  );
  const writer = createObjectCsvWriter({
    path: path.join("/tmp", "out.csv"),
    header: [
      { id: "gameid", title: "gameid" },
      { id: "url", title: "url" },
      { id: "price", title: "price" },
      { id: "currency", title: "currency" },
      { id: "enabled", title: "enabled" },
      { id: "show_from", title: "show_from" },
    ],
  });
  await writer.writeRecords(formattedProducts);
  console.log(`wrote matched CSV with ${formattedProducts.length} rows`);

  // Failed CSV
  const failedWriter = createObjectCsvWriter({
    path: path.join("/tmp", "failed.csv"),
    header: [
      { id: "id", title: "product_id" },
      { id: "handle", title: "handle" },
      { id: "barcode", title: "barcode" },
      { id: "metafield", title: "metafield" },
      { id: "price", title: "price" },
    ],
  });
  await failedWriter.writeRecords(
    failedProducts.map(p => ({
      id: p.id,
      handle: p.handle,
      barcode: p.barcode || '',
      metafield: p.metafield || '',
      price: p.price,
    }))
  );
  console.log(`wrote failed CSV with ${failedProducts.length} rows`);

  // Upload both (failed kept private)
  await put("bgg_products.csv", fileFromSync("/tmp/out.csv"), {access: "public", addRandomSuffix: false});
  await put("bgg_failed_products.csv", fileFromSync("/tmp/failed.csv"), {access: "public", addRandomSuffix: false});

  return;
}

export default async function handler(
  request: VercelRequest,
  response: VercelResponse
) {
  await main();
  return response.send("hello world");
}
