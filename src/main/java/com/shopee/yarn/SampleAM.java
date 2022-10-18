package com.shopee.yarn;


import com.beust.jcommander.JCommander;


public class SampleAM extends AbstractApplicationMaster {

    public SampleAM() {
        super();
    }

    public static void main(String[] args) {
        AbstractApplicationMaster am = new SampleAM();
        new JCommander(am, args);
        am.init(args);

        try {
            am.run();
        } catch (Exception e) {
            e.printStackTrace();
            am.shutdown();
            System.exit(-1);
        }
    }

}
