package thread;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.CountDownLatch;

public class threadRead {
    /**
     * 多线程读取文件测试
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        final int DOWN_THREAD_NUM = 10;//起10个线程去读取指定文件
        final String OUT_FILE_NAME = "倚天屠龙记.txt";
        final String keywords = "无忌";
        //jdk1.5线程辅助类，让主线程等待所有子线程执行完毕后使用的类，
        //另外一个解决方案：自己写定时器，个人建议用这个类
        CountDownLatch doneSignal = new CountDownLatch(DOWN_THREAD_NUM);
        RandomAccessFile[] outArr = new RandomAccessFile[DOWN_THREAD_NUM];
        try{
            long length = new File(OUT_FILE_NAME).length();
            System.out.println("文件总长度："+length+"字节");
            //每线程应该读取的字节数
            long numPerThred = length / DOWN_THREAD_NUM;
            System.out.println("每个线程读取的字节数："+numPerThred+"字节");
            //整个文件整除后剩下的余数
            long left = length % DOWN_THREAD_NUM;
            for (int i = 0; i < DOWN_THREAD_NUM; i++) {
                //为每个线程打开一个输入流、一个RandomAccessFile对象，

                //让每个线程分别负责读取文件的不同部分
                outArr[i] = new RandomAccessFile(OUT_FILE_NAME, "rw");
                if (i != 0) {
//
//                    isArr[i] = new FileInputStream("d:/勇敢的心.rmvb");
                    //以指定输出文件创建多个RandomAccessFile对象

                }
                if (i == DOWN_THREAD_NUM - 1) {
//                    //最后一个线程读取指定numPerThred+left个字节
//                  System.out.println("第"+i+"个线程读取从"+i * numPerThred+"到"+((i + 1) * numPerThred+ left)+"的位置");
                    new ReadThread(i * numPerThred, (i + 1) * numPerThred
                            + left, outArr[i],keywords,doneSignal).start();
                } else {
                    //每个线程负责读取一定的numPerThred个字节
//                  System.out.println("第"+i+"个线程读取从"+i * numPerThred+"到"+((i + 1) * numPerThred)+"的位置");
                    new ReadThread(i * numPerThred, (i + 1) * numPerThred,
                            outArr[i],keywords,doneSignal).start();
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
//      finally{
//
//      }
        //确认所有线程任务完成，开始执行主线程的操作
        try {
            doneSignal.await();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //这里需要做个判断，所有做read工作线程全部执行完。
        KeyWordsCount k = KeyWordsCount.getCountObject();
//      Map<String,Integer> resultMap = k.getMap();
        System.out.println("指定关键字出现的次数："+k.getCount());
    }
}
class KeyWordsCount {

    private static KeyWordsCount kc;

    private int count = 0;
    private KeyWordsCount(){

    }

    public static synchronized KeyWordsCount getCountObject(){
        if(kc == null){
            kc = new KeyWordsCount();
        }
        return kc;
    }

    public synchronized void  addCount(int count){
        System.out.println("增加次数："+count);
        this.count += count;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

}
class ReadThread extends Thread{

    //定义字节数组（取水的竹筒）的长度
    private final int BUFF_LEN = 256;
    //定义读取的起始点
    private long start;
    //定义读取的结束点
    private long end;
    //将读取到的字节输出到raf中  randomAccessFile可以理解为文件流，即文件中提取指定的一部分的包装对象
    private RandomAccessFile raf;
    //线程中需要指定的关键字
    private String keywords;
    //此线程读到关键字的次数
    private int curCount = 0;
    /**
     * jdk1.5开始加入的类，是个多线程辅助类
     * 用于多线程开始前统一执行操作或者多线程执行完成后调用主线程执行相应操作的类
     */
    private CountDownLatch doneSignal;
    public ReadThread(long start, long end, RandomAccessFile raf, String keywords, CountDownLatch doneSignal){
        this.start = start;
        this.end = end;
        this.raf  = raf;
        this.keywords = keywords;
        this.doneSignal = doneSignal;
    }

    public void run(){
        try {
            raf.seek(start);
            //本线程负责读取文件的大小
            long contentLen = end - start;
            //定义最多需要读取几次就可以完成本线程的读取
            long times = contentLen / BUFF_LEN+1;
            System.out.println(this.toString() + " 需要读的次数："+times);
            byte[] buff = new byte[BUFF_LEN];
            int hasRead = 0;
            String result = null;
            for (int i = 0; i < times; i++) {
                //之前SEEK指定了起始位置，这里读入指定字节组长度的内容，read方法返回的是下一个开始读的position
                hasRead = raf.read(buff);
                //如果读取的字节数小于0，则退出循环！ （到了字节数组的末尾）
                if (hasRead < 0) {
                    break;
                }
                result = new String(buff,"gb2312");
///             System.out.println(result);
                int count = this.getCountByKeywords(result, keywords);
                if(count > 0){
                    this.curCount += count;
                }
            }

            KeyWordsCount kc = KeyWordsCount.getCountObject();

            kc.addCount(this.curCount);

            doneSignal.countDown();//current thread finished! noted by latch object!
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public RandomAccessFile getRaf() {
        return raf;
    }

    public void setRaf(RandomAccessFile raf) {
        this.raf = raf;
    }

    public int getCountByKeywords(String statement,String key){
        return statement.split(key).length-1;
    }

    public int getCurCount() {
        return curCount;
    }

    public void setCurCount(int curCount) {
        this.curCount = curCount;
    }

    public CountDownLatch getDoneSignal() {
        return doneSignal;
    }

    public void setDoneSignal(CountDownLatch doneSignal) {
        this.doneSignal = doneSignal;
    }
}


