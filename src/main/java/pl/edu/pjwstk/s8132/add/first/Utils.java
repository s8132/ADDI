package pl.edu.pjwstk.s8132.add.first;

import java.io.File;

public class Utils {

    public String getFilePath(String file){
        return new File(getClass().getClassLoader().getResource(file).getFile()).getAbsolutePath();
    }
}
