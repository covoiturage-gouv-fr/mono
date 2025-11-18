{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };
  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
        pkgs = import nixpkgs { inherit system; };

        # Talisman pre-commit hook to detect secrets
        talisman = pkgs.stdenv.mkDerivation rec {
          pname = "talisman";

          # Get the hash using nix-prefetch-url https://...
          version = "1.37.0";
          sha256 = "1r57mb62n2aayzgxvcq56pk3aam30kmqni519w6g22qngfxyh2lf";

          # Binary URL from GitHub Releases
          src = pkgs.fetchurl {
            url = "https://github.com/thoughtworks/talisman/releases/download/v${version}/talisman_linux_amd64";
            sha256 = sha256;
          };

          dontUnpack = true;

          installPhase = ''
            mkdir -p $out/bin
            cp $src $out/bin/talisman
            chmod +x $out/bin/talisman
          '';

          meta = with pkgs.lib; {
            description = "A tool to detect secrets in your codebase";
            homepage = "https://github.com/thoughtworks/talisman";
            license = licenses.mit;
            platforms = [ "x86_64-linux" ];
            maintainers = with maintainers; [ ];
          };
        };

        sqlmesh = pkgs.writeShellApplication {
          name = "sqlmesh";
          runtimeInputs = with pkgs; [ uv ];
          text = ''
            exec uvx --from 'sqlmesh[postgres]' --python 3.13 sqlmesh "$@"
          '';
        };

        in {
          devShells.default = pkgs.mkShell {
            # expose pg_config for building psycopg2
            nativeBuildInputs = [ pkgs.postgresql_16.pg_config ];

            buildInputs = with pkgs; [
              # system packages
              p7zip
              just
              openssl
              jq
              minio-client

              # rpc infra
              nodejs_24
              postgresql_16
              deno

              # data stack
              dbt
              python313Packages.dbt-postgres
              uv
              sqlmesh # our custom sqlmesh wrapper

              # pre-commit hooks
              pre-commit
              talisman

              # misc
              gh
              yq-go
              zizmor
            ];
            shellHook = ''
              export PATH="$PWD/node_modules/.bin/:$PATH"
              export PRE_COMMIT_ALLOW_NO_CONFIG=1
              export GH_REPO=covoiturage-gouv-fr/mono
              export DENO_NO_UPDATE_CHECK=true
              export DENO_DIR="$PWD/api/.cache"
              export SEVEN_ZIP_BIN_PATH=$(which 7z)
              export LESS="-SRXF"
            '';
          };
        });
}
