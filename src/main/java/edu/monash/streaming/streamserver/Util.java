package edu.monash.streaming.streamserver;

import java.util.ArrayList;
import java.util.List;

public class Util {
    public static <T> List<List<T>> product(List<List<T>> partialResults, List<T> newMatches) {
        List<List<T>> output = new ArrayList<>();

        for (List<T> partialResult: partialResults) {
            for (T newMatch: newMatches) {
                List<T> newProduct = new ArrayList<>(partialResult);
                newProduct.add(newMatch);
                output.add(newProduct);
            }
        }

        return output;
    }
}
