package ir.sahab.nimbo.jimbo.fetcher;

import com.detectlanguage.DetectLanguage;
import com.detectlanguage.Result;
import com.detectlanguage.errors.APIError;
import com.google.common.base.Optional;
import com.optimaize.langdetect.DetectedLanguage;
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
    private static TextObjectFactory textObjectFactory = null;
    private static LanguageDetector languageDetector = null;
    private static List<LanguageProfile> languageProfiles = null;
    private static final double acceptProbability = 0.5;

    public static boolean isValid(Document document){
        if(banWords == null) {
            init();
        }
        return isEnglish(document.text()) && isNotBan(document);
    }

    static boolean isEnglishWithApi(String article){
        try {
            String language = DetectLanguage.simpleDetect(article);
            //List<Result> results = DetectLanguage.detect(article);
            //System.err.println(results.get(0).language);
            return language.equals("en");
        } catch (APIError apiError) {
            apiError.printStackTrace();
            return false;
        }

    }

    static boolean isEnglish(String article){
        TextObject textObject = textObjectFactory.forText(article);
        Optional<LdLocale> lang = languageDetector.detect(textObject);

        if (lang.isPresent()) {
            return lang.get().toString().equals("en");
        }
        double tmp = 1.0;
        for (DetectedLanguage detectedLanguage : languageDetector.getProbabilities(article)) {
            if (tmp < acceptProbability) return false;
            if (detectedLanguage.getProbability() > acceptProbability) {
                return detectedLanguage.getLocale().toString().equals("en");
            }
            tmp -= detectedLanguage.getProbability();
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

    private static void initLanguageDetect(){
        //        DetectLanguage.apiKey = "2806a039edb701c9b56b642b9a63a0ac";

        languageProfiles = null;
        try {
            languageProfiles = new LanguageProfileReader().readAllBuiltIn();
        } catch (IOException e) {
            e.printStackTrace();
        }
        languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
                .withProfiles(languageProfiles)
                .build();
        textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();

    }

    private static void initBanWords(){
        banWords = new ArrayList<>();
        String PROP_DIR = "banWords";
        Scanner inp = new Scanner(ClassLoader.getSystemResourceAsStream(PROP_DIR));
        while (inp.hasNext()){
            banWords.add(inp.next());
        }
    }
    static void init(){
        initLanguageDetect();
        initBanWords();
    }

}
