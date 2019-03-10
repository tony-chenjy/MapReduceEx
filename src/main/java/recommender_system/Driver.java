package recommender_system;

public class Driver {
	public static void main(String[] args) throws Exception {
		args = new String[]{"src/main/resources/recommender_system/input/userRating.txt",
							"src/main/resources/recommender_system/output/userRatingList",
							"src/main/resources/recommender_system/output/coOccurrenceMatrix",
							"src/main/resources/recommender_system/output/normalizeMatrix",
							"src/main/resources/recommender_system/output/multiplicationUnit",
							"src/main/resources/recommender_system/output/sum",
							"src/main/resources/recommender_system/output/compare",

							"src/main/resources/recommender_system/output/userAverageRating",
							"src/main/resources/recommender_system/output/multiplicationUnitWithAverage",
							"src/main/resources/recommender_system/output/sumWithAverage",
							"src/main/resources/recommender_system/output/compareWithAverage"};

		String userRating = args[0];
		String userRatingList = args[1];
		String coOccurrenceMatrix = args[2];
		String normalizeMatrix = args[3];
		String multiplicationUnit = args[4];
		String sum = args[5];
		String compare = args[6];

		String[] path1 = {userRating, userRatingList};
		String[] path2 = {userRatingList, coOccurrenceMatrix};
		String[] path3 = {coOccurrenceMatrix, normalizeMatrix};

		DataDividerByUser.main(path1);
		CoOccurrenceMatrixGenerator.main(path2);
		Normalize.main(path3);

		// without average rating
		String[] path4 = {normalizeMatrix, userRating, multiplicationUnit};
		String[] path5 = {multiplicationUnit, sum};
		String[] path6 = {userRating, sum, compare};

		Multiplication.main(path4);
		Sum.main(path5);
		Compare.main(path6);

		// with average rating
		String userAverageRating = args[7];
		String multiplicationUnitWithAverage = args[8];
		String sumWithAverage = args[9];
		String compareWithAverage = args[10];

		String[] path7 = {userRating, userAverageRating};
		String[] path8 = {normalizeMatrix, userRating, multiplicationUnitWithAverage, userAverageRating};
		String[] path9 = {multiplicationUnitWithAverage, sumWithAverage};
		String[] path10 = {userRating, sumWithAverage, compareWithAverage};

		AverageRating.main(path7);
		MultiplicationWithAverage.main(path8);
		Sum.main(path9);
		Compare.main(path10);

	}

}
