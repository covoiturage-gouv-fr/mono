type Connect = typeof Deno.connect;

const wrapped = Symbol.for("rpc.pg.noDelay");

export function withNoDelay(connect: Connect): Connect {
  const fn = async (options: Deno.ConnectOptions) => {
    const conn = await connect(options);
    if ("setNoDelay" in conn) conn.setNoDelay(true);
    return conn;
  };
  return Object.assign(fn, { [wrapped]: true }) as unknown as Connect;
}

// @db/postgres garde son socket privé et Deno n'active pas TCP_NODELAY par défaut
export function installNoDelay(): void {
  if (!(wrapped in Deno.connect)) Deno.connect = withNoDelay(Deno.connect);
}

installNoDelay();
