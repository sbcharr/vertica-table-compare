package com.sbcharr.mail;

import java.util.Properties;
import javax.mail.*;
import javax.mail.internet.*;
//import javax.activation.*;
import javax.mail.Session;
import javax.mail.Transport;


public class SendEmail {
    public static void main(String[] args) {
        String recipient = "";

        // email ID of  Sender.
        String sender = "";

        // using host as localhost
        String host = "localhost";
        Properties properties = new Properties();
        properties.setProperty("mail.smtp.host", host);

        Session session = Session.getDefaultInstance(properties);

        try {
            // MimeMessage object.
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(sender));

            message.addRecipient(Message.RecipientType.TO, new InternetAddress(recipient));

            // Set Subject: subject of the email
            message.setSubject("This is a test subject");

            // set body of the email.
            message.setText("This is a test mail");

            // Send email.
            Transport.send(message);
            System.out.println("Mail successfully sent");
        } catch (MessagingException mex) {
            mex.printStackTrace();
        }
    }
}

