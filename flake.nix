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
        let pkgs = import nixpkgs { inherit system; };
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
            ];
          };
        });
    };
}
