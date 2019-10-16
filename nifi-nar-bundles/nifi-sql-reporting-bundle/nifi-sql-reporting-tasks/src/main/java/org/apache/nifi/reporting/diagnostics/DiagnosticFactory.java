package org.apache.nifi.reporting.diagnostics;

import com.google.gson.reflect.TypeToken;
import com.google.gson.Gson;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.*;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class DiagnosticFactory {

    enum FileType{
        YAML, JSON;
    }

    public static List<Diagnostic> createDiagnostics(String diagnosticFile, String diagnosticFileType) throws Exception{

        FileType type = FileType.valueOf(diagnosticFileType.toUpperCase());
        if(type.equals(FileType.YAML)){
            return yamlToDiagnostic(diagnosticFile);
        }else if (type.equals(FileType.JSON)){
            return jsonToDiagnostic(diagnosticFile);
        }else{
            return null;
        }

    }

    private static List<Diagnostic> yamlToDiagnostic(String diagnosticFile) throws FileNotFoundException {
        List<Diagnostic> diagnostics = new ArrayList<>();
        Yaml yaml = new Yaml(new Constructor(Diagnostic.class));
        File yamlFile = new File(diagnosticFile);
        InputStream inputStream = new FileInputStream(yamlFile);
        for (Object object : yaml.loadAll(inputStream)) {
            if (object instanceof Diagnostic) {
                diagnostics.add((Diagnostic) object);
            }
        }
        return diagnostics;
    }

    private static List<Diagnostic> jsonToDiagnostic(String diagnosticFile) throws Exception{
        List<Diagnostic> diagnostics;
        Gson gson = new Gson();
        InputStreamReader isr = new InputStreamReader(new FileInputStream(diagnosticFile));
        Type diagnosticListType = new TypeToken<ArrayList<Diagnostic>>(){}.getType();
        diagnostics = gson.fromJson(isr,diagnosticListType);
        return diagnostics;
    }

}
