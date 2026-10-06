{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };
  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
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
            inherit sha256;
            url = "https://github.com/thoughtworks/talisman/releases/download/v${version}/talisman_linux_amd64";
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

        # NixOS : les wheels manylinux des venv uv cherchent libz, libstdc++ et libpq
        # dans les chemins système, absents sur NixOS. On les fournit au seul
        # interpréteur Python (wrapper) et non au shell entier : un LD_LIBRARY_PATH
        # global impose ces bibliothèques à tous les binaires du système, qui
        # plantent dès que leur glibc diffère (GLIBC_x.y not found, stack smashing).
        # --inherit-argv0 : le venv uv (.venv/bin/python -> wrapper) reste détecté.
        python313Nixos = pkgs.symlinkJoin {
          name = "python313-nixos";
          paths = [ pkgs.python313 ];
          nativeBuildInputs = [ pkgs.makeWrapper ];
          postBuild = ''
            for p in python python3 python3.13; do
              rm "$out/bin/$p"
              makeWrapper ${pkgs.python313}/bin/python3.13 "$out/bin/$p" \
                --inherit-argv0 \
                --prefix LD_LIBRARY_PATH : ${
                  pkgs.lib.makeLibraryPath [
                    pkgs.postgresql_17
                    pkgs.stdenv.cc.cc.lib
                    pkgs.zlib
                  ]
                }
            done
          '';
        };
      in
      {
        devShells.default = pkgs.mkShell {
          # expose pg_config for building psycopg2 in per-folder uv venvs
          nativeBuildInputs = [ pkgs.postgresql_17.pg_config ];

          buildInputs = with pkgs; [
            # system
            p7zip
            just
            openssl
            jq
            minio-client
            duckdb

            # rpc infra
            nodejs_24
            postgresql_17
            deno

            # data stack (python envs are managed per-folder via direnv + uv)
            cmake
            python313
            uv
            gdal # ogr2ogr : import des seeds géo du datalake (gpkg -> PostGIS)

            # pre-commit hooks
            pre-commit
            talisman

            # misc
            gh
            yq-go
            zizmor
          ];

          shellHook = ''
            # Les setup-hooks Python de Nix (python313, gdal, pre-commit) agrègent
            # leurs site-packages dans PYTHONPATH, qui masque alors les venv uv
            # per-folder et casse dbt/sqlfluff/pytest (ex : pyyaml sans SafeLoader).
            # Les envs Python sont gérés par dossier via uv : on repart de zéro.
            unset PYTHONPATH
            export PATH="$PWD/node_modules/.bin/:$PATH"
            export PRE_COMMIT_ALLOW_NO_CONFIG=1
            export GH_REPO=covoiturage-gouv-fr/mono
            export DENO_NO_UPDATE_CHECK=true
            export DENO_DIR="$PWD/api/.cache"
            export SEVEN_ZIP_BIN_PATH=$(which 7z)
            export LESS="-SRXF"
            if [ -L /run/current-system ]; then
              export PATH="${python313Nixos}/bin:$PATH"
            fi
          '';
        };
      }
    );
}
