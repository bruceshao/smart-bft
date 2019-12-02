package com.x.farmer.bft;

import com.x.farmer.bft.config.ViewController;
import com.x.farmer.bft.config.ViewProperties;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

public class LayerBoostrap {

    public static final String CONFIG_ARG = "-c";

    public static void main(String[] args) {
        try {
            // 加载入参中的配置文件
            InputStream inputStream = configFileInputStream(args);
            ViewController viewController = ViewProperties.resolve(inputStream);

            LayerInitializer layerInitializer = new LayerInitializer(viewController);

            layerInitializer.init();
            layerInitializer.start();

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    /**
     * 根据入参获取配置流信息
     *
     * @param args
     *     启动入参
     *
     * @return
     * @throws Exception
     */
    private static InputStream configFileInputStream(String[] args) throws Exception {

        if (args == null || args.length == 0) {
            return defaultConfigInputStream();
        }

        String filePath = null;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals(CONFIG_ARG)) {
                if (i + 1 < args.length) {
                    filePath = args[i+1];
                }
                break;
            }
        }

        if (filePath == null) {
            return defaultConfigInputStream();
        }

        return new FileInputStream(new File(filePath));
    }

    /**
     * 默认配置流信息
     *
     * @return
     */
    private static InputStream defaultConfigInputStream() {
        return LayerBoostrap.class.getResourceAsStream("/" + ViewProperties.BFT_CONFIG);
    }
}
