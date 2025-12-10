import { UnauthorizedException } from "@/ilos/common/index.ts";
import {
  CreateCredentialsResult,
  DeleteCredentialsResult,
  ReadCredentialsResult,
} from "@/pdc/services/auth/dto/Credentials.ts";
import { newExceptionFromHttpStatus } from "../../src/ilos/common/newExceptionFromHttpStatus.ts";
import { env } from "./config.ts";
import { faker } from "./faker.ts";
import { RPCResponse } from "./types.ts";

export type HTTPResponse<T = string | object | null> = {
  ok: boolean;
  status: number;
  statusText: string;
  url: string;
  redirected: boolean;
  headers: Headers;
  body: T;
};

export class API {
  #baseUrl = env("APIE2E_API_URL", "http://localhost:8080");
  #apiVersion = env("APIE2E_API_VERSION", "v3");
  #accessToken: string | null = null;
  #sessionCookie: string | null = null;

  get token(): string | null {
    return this.#accessToken;
  }

  /**
   * -------------------------------------------------------------------------------------------------------------------
   * AUTHENTICATION
   * -------------------------------------------------------------------------------------------------------------------
   */

  // Get a temporary access token using the default access key and secret key.
  public async authenticate(access_key: string, secret_key: string): Promise<void> {
    try {
      const url = new URL(`/${this.#apiVersion}/auth/access_token`, this.#baseUrl);

      const response = await fetch(url, {
        method: "POST",
        headers: {
          "Accept": "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ access_key, secret_key }),
      });

      const body = await this.getBody(response) as { access_token?: string };

      if (!response.ok) {
        const { error } = response.body as { error?: string } || response.body;
        throw newExceptionFromHttpStatus(response.status, error || response.statusText);
      }

      if (!body.access_token || typeof body.access_token !== "string") {
        throw new Error(`Invalid access token for ${access_key}`);
      }

      this.#accessToken = body.access_token;
    } catch (e) {
      if (e instanceof Error) {
        console.error(e.message);
      }

      throw e;
    }
  }

  // Logout and clear the session cookie and access token.
  public async logout(): Promise<void> {
    this.clearSessionCookie();
    this.clearAccessToken();
  }

  // Connect to the API using email and password, and retrieve a session cookie.
  public async login<T = unknown>(email: string, password: string): Promise<T> {
    await this.logout();

    const loginResponse = await this.post<RPCResponse<T>>("/auth/test/login", { email, password });
    if (!loginResponse.ok) {
      throw new UnauthorizedException();
    }

    const cookie = loginResponse.headers.get("set-cookie");
    if (!cookie) {
      throw new Error("Failed to get session cookie");
    }

    this.#sessionCookie = cookie.split(";")[0];

    return loginResponse.body as T;
  }

  public clearSessionCookie(): void {
    this.#sessionCookie = null;
  }

  public clearAccessToken(): void {
    this.#accessToken = null;
  }

  public async legacyAuthenticate(email: string, password: string): Promise<void> {
    // Login
    await this.login(email, password);

    // Create a new application and get the access token
    const appResponse = await this.post("/applications", {
      name: `APIE2E Application ${faker.string.nanoid()}`,
    });

    if (!appResponse.ok) {
      throw new Error(`Failed to create application: ${appResponse.statusText}`);
    }

    const { token } = appResponse.body as { application: unknown; token: string };
    if (!token || typeof token !== "string") {
      throw new Error("Invalid access token received from application creation");
    }

    this.#accessToken = token;
  }

  /**
   * -------------------------------------------------------------------------------------------------------------------
   * CREDENTIALS MANAGEMENT
   * -------------------------------------------------------------------------------------------------------------------
   */

  public async readCredentials(operator_id: number): Promise<ReadCredentialsResult> {
    const params = new URLSearchParams({ operator_id: String(operator_id) });
    const response = await this.get<ReadCredentialsResult>(`/${this.#apiVersion}/auth/credentials`, params);

    if (!response.ok) {
      const { error } = response.body as { error?: string } || response.body;
      throw newExceptionFromHttpStatus(response.status, error || response.statusText);
    }

    return response.body;
  }

  public async createCredentials(operator_id: number): Promise<CreateCredentialsResult> {
    const response = await this.post<CreateCredentialsResult>(
      `/${this.#apiVersion}/auth/credentials`,
      { operator_id },
    );

    if (!response.ok) {
      const { error } = response.body as { error?: string } || response.body;
      throw newExceptionFromHttpStatus(response.status, error || response.statusText);
    }

    return response.body;
  }

  public async deleteCredentials(operator_id: number, token_id: string): Promise<DeleteCredentialsResult> {
    const params = new URLSearchParams({ operator_id: String(operator_id), token_id });
    const response = await this.delete<DeleteCredentialsResult>(`/${this.#apiVersion}/auth/credentials`, params);

    if (!response.ok) {
      throw newExceptionFromHttpStatus(response.status, response.statusText);
    }

    return;
  }

  /**
   * -------------------------------------------------------------------------------------------------------------------
   * HTTP REQUESTS METHODS
   * -------------------------------------------------------------------------------------------------------------------
   */

  public async get<T extends string | object | null>(
    url: string | URL,
    searchParams?: URLSearchParams,
  ): Promise<HTTPResponse<T>> {
    return await this.request<T>("GET", url, undefined, searchParams);
  }

  public async post<T extends string | object | null>(
    url: string | URL,
    body: object | BodyInit,
    searchParams?: URLSearchParams,
  ): Promise<HTTPResponse<T>> {
    return await this.request("POST", url, body, searchParams);
  }

  public async delete<T extends string | object | null | void>(
    url: string | URL,
    searchParams?: URLSearchParams,
  ): Promise<HTTPResponse<T>> {
    return await this.request<T>("DELETE", url, undefined, searchParams);
  }

  public async request<T extends string | object | null | void>(
    method: "GET" | "POST" | "DELETE",
    url: string | URL,
    body?: object | BodyInit,
    searchParams?: URLSearchParams,
  ): Promise<HTTPResponse<T>> {
    const input = url instanceof URL ? url : new URL(url, this.#baseUrl);

    if (searchParams) {
      input.search = searchParams.toString();
    }

    const init: RequestInit = {
      method,
      headers: {
        "Accept": "application/json",
        "Content-Type": "application/json",
      },
    };

    if (this.#sessionCookie) {
      init.headers = {
        ...init.headers,
        Cookie: this.#sessionCookie,
      };
    }

    if (this.#accessToken) {
      init.headers = {
        ...init.headers,
        Authorization: `Bearer ${this.#accessToken}`,
      };
    }

    if (method === "POST") {
      init.body = typeof body === "object" ? JSON.stringify(body) : body;
    }

    try {
      const response = await fetch(input, init);
      const body = await this.getBody<T>(response);

      return {
        ok: response.ok,
        status: response.status,
        statusText: response.statusText,
        url: response.url,
        redirected: response.redirected,
        headers: response.headers,
        body,
      };
    } catch (e) {
      if (e instanceof Error) {
        console.error(e.message);
      }

      return {
        ok: false,
        status: 0,
        statusText: "Network error",
        url: input.toString(),
        redirected: false,
        headers: new Headers(),
        body: null as T,
      };
    }
  }

  private async getBody<T extends string | object | null | void>(response: Response): Promise<T> {
    if (response.status === 204) return null as T;

    const contentType = response.headers.get("content-type");

    if (contentType && contentType.includes("application/json")) {
      return await response.json() as T;
    }

    return await response.text() as T;
  }
}
