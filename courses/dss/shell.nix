{ pkgs ? import <nixpkgs> { }, }:

pkgs.mkShell rec {
  name = "dss";

  buildInputs = with pkgs; [
    protobuf
  ];

  PROTOC = "${pkgs.protobuf}/bin/protoc";
}
