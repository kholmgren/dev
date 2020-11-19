package io.kettil.fn.deploy.model;

import lombok.Data;

import java.io.File;

@Data
public class Manifest {
    private File location;
    private String name;
    private String runtime;
}
