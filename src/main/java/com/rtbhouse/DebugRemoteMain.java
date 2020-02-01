package com.rtbhouse;

public class DebugRemoteMain {

    public static void main(String[] args) throws InterruptedException {
        int i = 0;
        while (true) {
            i++;
            System.out.println("working: " + i);
            Thread.sleep(5_000);
        }
    }
}
