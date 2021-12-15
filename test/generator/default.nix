# This creates two derivations which depend on each other.
with import <nixpkgs> { };

let
  txt = writeText "txt" ''
    Hello, World!
  '';
in
stdenv.mkDerivation {
  name = "hello";
  dontUnpack = true;
  dontBuild = true;
  installPhase = ''
    mkdir -p $out
    ln -sfn ${txt} $out/txt
  '';
}
