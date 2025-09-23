import { Strapi } from "@strapi/strapi";

type FlowFn = () => Promise<boolean>;

const flows: Record<string, FlowFn> = {
  flush_cache: async () => {
    const url = `https://api.covoiturage.beta.gouv.fr/cache`;
    const params = {
      method: "DELETE",
      headers: {
        "X-Route-Cache-Auth": `${process.env.FLOW_FLUSH_CACHE_TOKEN}`,
      },
    };

    if (!process.env.FLOW_FLUSH_CACHE_TOKEN) {
      console.debug("flush_cache", url, params);
      return true;
    }

    const response = await fetch(url, params);
    if (!response.ok) {
      return false;
    }
    return true;
  },
  deploy: async () => {
    const url =
      `https://api.github.com/repos/covoiturage-gouv-fr/mono/actions/workflows/deploy-frontends-public.yml/dispatches`;
    const params = {
      method: "POST",
      headers: {
        "Authorization": `token ${process.env.FLOW_DEPLOY_GH_TOKEN}`,
        "Accept": "application/vnd.github.v3+json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        ref: "main",
      }),
    };

    if (!process.env.FLOW_DEPLOY_GH_TOKEN) {
      console.debug("deploy", url, params);
      return true;
    }

    const response = await fetch(url, params);

    if (!response.ok) {
      const errorMsg = await response.text();
      console.error(errorMsg);
      return false;
    }

    return true;
  },
};

export default ({ strapi }: { strapi: Strapi }) => ({
  list() {
    return Object.keys(flows);
  },
  async run(id: string) {
    if (!(id in flows)) {
      return;
    }
    const success = await flows[id]();
    return {
      success,
      flow: id,
    };
  },
});
