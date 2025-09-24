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

        talisman = pkgs.stdenv.mkDerivation rec {
          pname = "talisman";
          version = "1.34.0";

          # Binary URL from GitHub Releases
          src = pkgs.fetchurl {
            url = "https://github.com/thoughtworks/talisman/releases/download/v${version}/talisman_linux_amd64";
            sha256 = "89f730cb4f1cd3143f0e4c13ec2e21842d61e758b10439ad6146d41792f865b5";
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
        in {
          devShells.default = pkgs.mkShell {
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
              pre-commit
              python312Packages.dbt-postgres
              uv

              # pre-commit hooks
              pre-commit
              talisman

              # misc
              gh
              yq-go
            ];
            shellHook = ''
              export PATH="$PWD/node_modules/.bin/:$PATH"
              export PRE_COMMIT_ALLOW_NO_CONFIG=1
              export GH_REPO=covoiturage-gouv-fr/mono
              export DENO_NO_UPDATE_CHECK=true
              export SEVEN_ZIP_BIN_PATH=$(which 7z)
              export LESS="-SRXF"
            '';
          };
        });
}
