//package com.github.mk23.imagesizer;

import java.awt.BorderLayout;
import java.awt.Container;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;

import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JSlider;
import javax.swing.UIManager;

public class ImageSizer extends WindowAdapter implements ActionListener {
    private final String[] files;

    public ImageSizer(String[] files) {
        this.files = files;
    }

    private JComponent createScale() {
        JSlider scale = new JSlider(0, 300, 30);
        scale.setMinorTickSpacing(10);
        scale.setSnapToTicks(true);

        return scale;
    }

    private JComponent createFiles() {
        JList<String> files = new JList<String>(this.files);
        
        return files;
    }

    public void actionPerformed(ActionEvent e) {
        String command = e.getActionCommand();
        System.out.println("action: " + command);
    }

    public static void main(String[] args) {
        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                try {
                    UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
                } catch (Exception e) {
                }

                JFrame frame = new JFrame("ImageSizer");
                frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

                ImageSizer sizer = new ImageSizer(args);

                Container view = frame.getContentPane();
                view.add(sizer.createScale(), BorderLayout.CENTER);
                view.add(sizer.createFiles(), BorderLayout.PAGE_END);

                frame.pack();
                frame.setLocationRelativeTo(null);
                frame.setVisible(true);
            }
        });
    }


}
