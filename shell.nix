{ pkgs ? import <nixpkgs> }:

with pkgs;
mkShell {
  buildInputs = [ go_1_19 ];
  shellHook = ''
    # ...
  '';
}
