package ir.sahab.nimbo.jimbo.fetcher;

import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Validate {

    static List<String> banWords = null;
    private static String PROP_DIR = "banWords";
    public static boolean isValid(Document document){
        if(banWords == null) {
            initBans();
        }
        return isEnglish(document.text()) && isNotBan(document);
    }

    static boolean isEnglish(String article){
        List<LanguageProfile> languageProfiles = null;
        try {
            languageProfiles = new LanguageProfileReader().readAllBuiltIn();
        } catch (IOException e) {
            e.printStackTrace();
        }
        LanguageDetector languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
                .withProfiles(languageProfiles)
                .build();
        TextObjectFactory textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();
        TextObject textObject = textObjectFactory.forText(article);
        try {
            LdLocale lang = languageDetector.detect(textObject).get();
            if (lang.toString().equals("en"))
                return true;
        } catch (IllegalStateException e) {
            return false;
        }
        return false;
    }

    static boolean isNotBan(Document document) {
        for(String word : banWords) {
            if(document.title().contains(word) || document.head().text().contains(word)){
                return false;
            }
        }
        return true;

    }
    private static void initBans(){
        banWords = new ArrayList<>();
        Scanner inp = new Scanner(ClassLoader.getSystemResourceAsStream(PROP_DIR));
        while (inp.hasNext()){
            banWords.add(inp.next());
        }
    }

}
