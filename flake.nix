{
  description = "Development environment for Veylor";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      devShells = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
          h3spec = if system == "x86_64-linux" then
            pkgs.runCommand "h3spec-0.1.14" {
              src = pkgs.fetchurl {
                url = "https://github.com/kazu-yamamoto/h3spec/releases/download/v0.1.14/h3spec-linux-x86_64";
                hash = "sha256-fy4yMnFaP2/1IS1Aiyo+dNaqPKBvlu/Kmk5dxV88aGA=";
              };
            } ''
              mkdir -p $out/bin
              install -m755 $src $out/bin/h3spec
            ''
          else null;
        in {
          default = pkgs.mkShell {
            packages = with pkgs; [
              rustc
              cargo
              rustfmt
              clippy
              rust-analyzer
              clang
              clang-tools
              lldb
              cmake
              perl
              just
              pkg-config
              openssl
              sqlite
              zig
              go
              h2spec
              nghttp2
              curlHTTP3
              jq
              valgrind
              flamegraph
              cargo-flamegraph
              heaptrack
            ] ++ nixpkgs.lib.optionals (h3spec != null) [ h3spec ];
          };
        });
    };
}
