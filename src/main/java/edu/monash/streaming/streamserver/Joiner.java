package edu.monash.streaming.streamserver;

import edu.monash.streaming.streamserver.algorithm.*;

import java.util.ArrayList;
import java.util.List;

public class Joiner {
    private final Receiver receiver;
    private final JoinType joinType;
    private final long W;
    private final long T;
    private final long Te;

    public Joiner(Receiver receiver,
                  JoinType joinType,
                  long W,
                  long T,
                  long Te) {
        this.receiver = receiver;
        this.joinType = joinType;
        this.W = W;
        this.T = T;
        this.Te = Te;
    }

    public void start() throws Exception {
        Join join = null;

        switch (joinType) {
            case MJOIN:
                join = new MJoin(receiver, W);
                break;
            case AMJOIN:
                join = new AMJoin(receiver, W);
                break;
            case TSWA4MJOIN:
                // Te -> N
                join = new TSWA4MJoin(receiver, W, T, Te);
                break;
            case TSWA4OJOIN:
                // Te -> N
                join = new TSWA4OJoin(receiver, W, T, Te);
                break;
            case TSWA5JOIN:
                join = new TSWA5Join(receiver, W, T, Te);
                break;
            case TSWA6JOIN:
                join = new TSWA6Join(receiver, W, T, Te);
                break;
            case TSWA7JOIN:
                join = new TSWA7Join(receiver, W, T, Te);
                break;
            default:
                throw new Exception("Algorithm not found");
        }

        join.start();
    }

    public static <T> List<List<T>> product(List<List<T>> partialResults, List<T> newMatches) {
        List<List<T>> output = new ArrayList<>();

        if (partialResults.size() > 0) {
            for (List<T> partialResult : partialResults) {
                for (T newMatch : newMatches) {
                    List<T> newProduct = new ArrayList<>(partialResult);
                    newProduct.add(newMatch);
                    output.add(newProduct);
                }
            }
        } else {
            output.add(newMatches);
        }

        return output;
    }

    public static <T> List<List<T>> product(List<List<T>> input) {
        List<List<T>> result= new ArrayList<>();

        if (input.isEmpty()) {  // If input a empty list
            result.add(new ArrayList<T>());// then add empty list and return
            return result;
        } else {
            List<T> head = input.get(0);//getCounter the first list as a head
            List<List<T>> tail= product(input.subList(1, input.size()));//recursion to calculate a tail list
            for (T h : head) {//we merge every head element with every tail list.
                for (List<T> t : tail) {
                    List<T> resultElement = new ArrayList<T>();
                    resultElement.add(h);
                    resultElement.addAll(t);
                    result.add(resultElement);
                }
            }
        }
        return result;
    }
}
