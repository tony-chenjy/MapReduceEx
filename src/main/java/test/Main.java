package test;

/**
 * @author cm
 * @date 2019/2/6 0006 21:53
 */
public class Main {
    public static void main(String[] args) {
        String line = "This is   the content of document1 ";
        String[] words = line.split("\\s+");

        System.out.println(words.length); // TODO
        for (String word : words) {
            System.out.println(word); // TODO
        }
    }
}
