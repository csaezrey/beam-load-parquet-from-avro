package org.example.functions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.codec.binary.Base64;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import static org.example.utils.Constants.*;

public class EncryptFunction extends DoFn <GenericRecord, GenericRecord>{

    Schema schema;
    String fieldEncrypt;

    public EncryptFunction(String schema, String fieldEncrypt){
        this.schema= new Schema.Parser().parse(schema);
        this.fieldEncrypt = fieldEncrypt;
    }

    @ProcessElement
    public void processElement(ProcessContext context){

        GenericRecord record = context.element();
        GenericRecord generic = new GenericData.Record(schema);


        for (Schema.Field field : record.getSchema().getFields()) {
            String fieldKey = field.name();
            if(fieldKey.equalsIgnoreCase(fieldEncrypt)){
                generic.put(fieldKey, encrypt(record.get(fieldKey).toString()));
            }else{
                generic.put(fieldKey, record.get(fieldKey).toString());
            }
        }

        context.output(generic);
    }

public String encrypt(String toBeEncrypt)  {
    IvParameterSpec ivParameterSpec;
    SecretKeySpec secretKeySpec;
    byte[] encrypted = null;
    try{
        Cipher cipher = Cipher.getInstance(CIPHER_INSTANCE);
        ivParameterSpec = new IvParameterSpec(SECRET_KEY_1.getBytes(UTF_8));
        secretKeySpec = new SecretKeySpec(SECRET_KEY_2.getBytes(UTF_8), AES);
        cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, ivParameterSpec);
        encrypted = cipher.doFinal(toBeEncrypt.getBytes());
    }catch ( NoSuchPaddingException | NoSuchAlgorithmException | UnsupportedEncodingException | IllegalBlockSizeException | BadPaddingException | InvalidAlgorithmParameterException | InvalidKeyException e){
        System.out.println(e.getMessage());
    }
    return Base64.encodeBase64String(encrypted);
    }
}
