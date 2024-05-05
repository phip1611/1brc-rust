{ pkgs ? import <nixpkgs> { } }:

let
  jdk = pkgs.jdk21;
  maven = pkgs.maven.override {
    inherit jdk;
  };
in
pkgs.mkShell {
  packages = [
    jdk
    maven
    pkgs.rustup
  ];

  JAVA_HOME = jdk;
}
