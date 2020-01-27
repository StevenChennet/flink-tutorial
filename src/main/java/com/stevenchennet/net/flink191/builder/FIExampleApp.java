package com.stevenchennet.net.flink191.builder;

import java.util.function.Consumer;
import java.util.function.Function;

public class FIExampleApp {
    public static void main(String[] args) {
        //FuncEx();

        ConsEx();;

    }

    private static void FuncEx() {
        Function<Integer, Long> f1 = i -> {
            System.out.println("f1 called.");
            return i * 2L;
        };
        Function<Long, String> f2 = i -> {
            System.out.println("f2 called.");
            return "s" + i;};

        Function<Integer, String> f3 = f1.andThen(f2);
        System.out.println(f3.apply(2));

        System.out.println("-----------------");

        Function<Integer, String> f5 = f2.compose(f1);
        System.out.println(f5.apply(3));
    }

    private static void ConsEx(){
        Consumer<Integer> c1 = i->{
            System.out.println("c1 called");
        };

        Consumer<Integer> c2 = i->{
            System.out.println("c2 called");
        };

        Consumer<Integer> c3 = c1.andThen(c2);
        c3.accept(4);
    }


}
