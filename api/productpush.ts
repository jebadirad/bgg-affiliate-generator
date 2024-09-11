import "dotenv/config";
import neatCsv from "neat-csv";

import { waitUntil } from "@vercel/functions";
import type { VercelRequest, VercelResponse } from "@vercel/node";

import * as fs from "node:fs";

import * as path from "node:path";
import { createObjectCsvWriter } from "csv-writer";
import "@shopify/shopify-api/adapters/node";
import { shopifyApi, LATEST_API_VERSION } from "@shopify/shopify-api";
//@ts-ignore
import { fetch, CookieJar } from "node-fetch-cookies";
import { fileFromSync, FormData, Response } from "node-fetch";
import { stringify } from "node:querystring";
const shopify = shopifyApi({
  // The next 4 values are typically read from environment variables for added security
  apiKey: process.env.api_key,
  apiSecretKey: process.env.api_secret as string,
  scopes: ["read_products"],
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

const bggUsername = process.env.bggaccountusername;
const bggPw = process.env.bggaccountpw;
const bggDomain = "boardgamegeek.com";

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
    handle: string;
    metafield: string | null;
    totalInventory: number;
    price: number;
  }[]
> => {
  const queryString = `{
        products(first: 20, ${after ? `after: "${after}"` : ""} 
            query: "status:Active AND ((product_type:'Board Game') OR (product_type:'Board Games') OR (product_type:'Card Game') OR (product_type:'Dice Game') OR (product_type:'Non-Collectible Card Games') OR (tag:boardgame OR (tag:rpg AND -tag:rpg dice sets) OR tag:miniatures))"
        ){
            edges {
                node {
                    handle
                    totalInventory
                    priceRangeV2 {
                        minVariantPrice {
                            amount
                        }
                    }
                    metafield(key: "bgg_game_id", namespace: "custom"){
                        value
                    }
                }
            }
            pageInfo {
                hasNextPage
                endCursor
            }
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
            handle: string;
            totalInventory: number;
            metafield: {
              value: string;
            };
            priceRangeV2: {
              minVariantPrice: {
                amount: number;
              };
            };
          };
        }>;
        pageInfo: {
          hasNextPage: boolean;
          endCursor: string;
        };
      };
    };
  }>({
    data: queryString,
  });

  const formattedProducts = products.edges.map(
    ({ node: { handle, metafield, priceRangeV2, totalInventory } }) => {
      return {
        handle,
        totalInventory: totalInventory,
        metafield: metafield ? metafield.value : null,
        price:
          Math.round(
            (priceRangeV2.minVariantPrice.amount -
              priceRangeV2.minVariantPrice.amount * 0.05 +
              Number.EPSILON) *
              100
          ) / 100,
      };
    }
  );
  console.log(JSON.stringify(products));
  if (products.pageInfo.hasNextPage) {
    const nextPage = await getAllProducts({
      after: products.pageInfo.endCursor,
    });
    return formattedProducts.concat(nextPage);
  }
  return formattedProducts;
};

const prepareToUploadToShopify = async () => {
  const mutationString = `
    mutation generateStagedUploads {
  stagedUploadsCreate(input: [
    {
      filename: "bgg_file_list",
      mimeType: "text/csv",
      resource: FILE,
    },
  ])
    {
    stagedTargets {
      url
      resourceUrl
      parameters {
        name
        value
      }
    }
    userErrors {
      field, message
    }
  }
}
  `;

  const res = await client.query<{
    data: {
      stagedUploadsCreate: {
        stagedTargets: {
          url: string;
          resourceUrl: string;
          parameters: {
            name: string;
            value: string;
          }[];
        }[];
        userErrors: {
          field: string[];
          message: string;
        }[];
      };
    };
  }>({
    data: mutationString,
  });
  const { stagedUploadsCreate } = res.body.data;
  if (stagedUploadsCreate.userErrors.length) {
    //no errors
    throw new Error(JSON.stringify(stagedUploadsCreate.userErrors));
  }
  return stagedUploadsCreate.stagedTargets;
};

const updateFile = async ({
  originalSource,
  fileId,
}: {
  originalSource: string;
  fileId: string;
}) => {
  const mutation = `
  mutation FileUpdate($input: [FileUpdateInput!]!) {
  fileUpdate(files: $input) {
    userErrors {
      code
      field
      message
    }
    files {
      alt
    }
  }
}`;

  const res = await client.request(mutation, {
    variables: {
      input: [{ id: fileId, originalSource }],
    },
  });
};

export async function main() {
  const cookieJar = new CookieJar();
  const products = await getAllProducts({});
  const primaries = (await neatCsv(fs.createReadStream(PATH_TO_MAIN), {
    headers: ["objectid", "name"],
  })) as Array<{ objectid: string; name: string }>;
  const rpgs = (await neatCsv(fs.createReadStream(PATH_TO_RPG), {
    headers: ["objectid", "name"],
  })) as Array<{ objectid: string; name: string }>;
  const missMatcheddProducts: {
    handle: string;
    metafield: string | null;
    price: number;
  }[] = [];
  const matchedProducts = products.filter<{
    handle: string;
    metafield: string;
    totalInventory: number;
    price: number;
  }>(
    (
      val
    ): val is {
      handle: string;
      metafield: string;
      price: number;
      totalInventory: number;
    } => {
      if (val.metafield) {
        const find = primaries.find((v) => {
          return v.objectid === val.metafield;
        });
        if (find) {
          return true;
        } else {
          const findRPG = rpgs.find((v) => {
            return v.objectid === val.metafield;
          });
          if (findRPG) {
            return true;
          }
        }
      }
      missMatcheddProducts.push(val);
      return false;
    }
  );
  const formattedProducts: Array<BGGUpload> = matchedProducts.map(
    ({ metafield, price, handle, totalInventory }) => {
      return {
        currency: "USD",
        enabled: totalInventory > 0 ? 1 : 0,
        gameid: metafield,
        price,
        show_from: 1,
        url: affiliateUrlBuilder({ handle }),
      };
    }
  );
  const writer = createObjectCsvWriter({
    path: path.join("/tmp", "out.csv"),
    header: [
      {
        id: "gameid",
        title: "gameid",
      },
      {
        id: "url",
        title: "url",
      },
      {
        id: "price",
        title: "price",
      },
      {
        id: "currency",
        title: "currency",
      },
      {
        id: "enabled",
        title: "enabled",
      },
      {
        id: "show_from",
        title: "show_from",
      },
    ],
  });
  await writer.writeRecords(formattedProducts);
  console.log("done");

  const failedWriter = createObjectCsvWriter({
    path: path.join("/tmp", "failed.csv"),
    header: [
      {
        id: "handle",
        title: "handle",
      },
      {
        id: "metafield",
        title: "metafield",
      },
      {
        id: "price",
        title: "price",
      },
    ],
  });
  await failedWriter.writeRecords(missMatcheddProducts);
  const preppedUpload = await prepareToUploadToShopify();
  for (const prep of preppedUpload) {
    const { parameters, resourceUrl, url } = prep;
    console.log(resourceUrl);
    console.log(url);
    const putHeaders: Record<string, any> = {};
    parameters.forEach(({ name, value }) => {
      console.log(name, value);
      putHeaders[name] = value;
    });
    //const blob = new Blob([await fs.promises.readFile("/tmp/out.csv")]);
    const fileStream = fs.createReadStream("/tmp/out.csv");
    //formData.append("file", blob);

    const sendFile: Response = await fetch(cookieJar, url, {
      method: "PUT",
      headers: putHeaders,
      body: fileStream,
    });
    if (sendFile.ok) {
      await updateFile({
        originalSource: resourceUrl,
        fileId: "gid://shopify/GenericFile/39399716520220",
      });
    }
  }
  const missPrepProduct = await prepareToUploadToShopify();
  for (const prep of missPrepProduct) {
    const { parameters, resourceUrl, url } = prep;
    console.log(resourceUrl);
    console.log(url);
    const putHeaders: Record<string, any> = {};
    parameters.forEach(({ name, value }) => {
      console.log(name, value);
      putHeaders[name] = value;
    });
    //const blob = new Blob([await fs.promises.readFile("/tmp/out.csv")]);
    const fileStream = fs.createReadStream("/tmp/failed.csv");
    //formData.append("file", blob);

    const sendFile: Response = await fetch(cookieJar, url, {
      method: "PUT",
      headers: putHeaders,
      body: fileStream,
    });
    if (sendFile.ok) {
      await updateFile({
        originalSource: resourceUrl,
        fileId: "gid://shopify/GenericFile/39400384200988",
      });
    }
  }
  return;
}

export default async function handler(
  request: VercelRequest,
  response: VercelResponse
) {
  await main();
  return response.send("hello world");
}
