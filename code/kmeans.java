import java.util.ArrayList;
import java.util.Random;

public class KMeans {
	
	private ArrayList<Point> points;
	private int clusters;

	public KMeans(ArrayList<Point> points, int clusters) {
		this.points = points;
		this.clusters = clusters;
	}

	public void find() {
		ArrayList<Point> clusterPoints = new ArrayList<Point>();
		double best_distance = -1;
		while(true) {
			clusterPoints = getNewClusterPoints();
			int total_points = 0;
			double total_distances = 0;
			for(Point point : points) {
				Point nearest = clusterPoints.get(0);
				double t_distance = getDistance(point, clusterPoints.get(0));
				for(Point clusterPoint : clusterPoints) {
					double d = getDistance(point, clusterPoint);
					if(d < t_distance) {
						nearest = clusterPoint;
						t_distance = d;
					}
				}
				total_points += 1;
				total_distances += t_distance;
			}
			double d = total_distances / (double) total_points;
			if(best_distance == -1 || d < best_distance) {
				System.out.print("New best: ");
				for(Point p : clusterPoints) {
					System.out.print(p.toString() + " ");
				}
				System.out.print("with average distance equal to " + d + "\n");
				best_distance = d;
			}
		}
	}

	private ArrayList<Point> getNewClusterPoints() {
		ArrayList<Point> boundaries = getBoundaries();
		Range range_x = new Range(boundaries.get(1).x, boundaries.get(0).x);
		Range range_y = new Range(boundaries.get(1).y, boundaries.get(0).y);
		ArrayList<Point> newClusterPoints = new ArrayList<Point>();
		for(int i = 0; i < clusters; i += 1) {
			newClusterPoints.add(new Point(range_x, range_y));
		}
		return newClusterPoints;
	}

	private double getDistance(Point point_a, Point point_b) {
		double x_distance = Math.abs(point_a.x - point_b.x);
		double y_distance = Math.abs(point_a.y - point_b.y);
		return Math.sqrt(x_distance*x_distance + y_distance*y_distance);
	}

	private ArrayList<Point> getBoundaries() {
		double xmin = 0;
		double xmax = 0;
		double ymin = 0;
		double ymax = 0;
		ArrayList<Point> boundaries = new ArrayList<Point>();
		for(int i = 0; i < points.size(); i += 1) {
			Point point = points.get(i);
			double x = point.x;
			double y = point.y;
			if(i == 0) {
				xmin = x;
				xmax = x;
				ymin = y;
				ymax = y;
			} else {
				if(x < xmin) {
					xmin = x;
				}
				if(x > xmax) {
					xmax = x;
				}
				if(y < ymin) {
					ymin = y;
				} 
				if(y > ymax) {
					ymax = y;
				}
			}
		}
		boundaries.add(new Point(xmin, ymin));
		boundaries.add(new Point(xmax, ymax));
		return boundaries;
	}

}

class Point {

	final double x;
	final double y;

	public Point(double x, double y) {
		this.x = x;
		this.y = y;
	}

	public Point(Range range_x, Range range_y) {
		this.x = (new Random()).nextDouble()*(range_x.max-range_x.min)+range_x.min;
		this.y = (new Random()).nextDouble()*(range_y.max-range_y.min)+range_y.min;
	}

	public Point getCopy() {
		return (new Point(this.x, this.y));
	}

	public String toString() {
		return "(" + x + "," + y + ")";
	}

}

class Range {

	final double min;
	final double max;

	public Range(double min, double max) {
		this.min = min;
		this.max = max;
	}

}

class Main {

	public static void main(String[] args) {
		ArrayList<Point> points = new ArrayList<Point>();
		points.add(new Point(9.0,8.0));
		points.add(new Point(9.5,8.0));
		points.add(new Point(8.5,8.5));
		points.add(new Point(9.0,9.0));
		points.add(new Point(0.5,1.0));
		points.add(new Point(1.5,1.0));
		points.add(new Point(1.0,0.5));
		points.add(new Point(0.5,2.0));
		points.add(new Point(0.5,8.5));
		points.add(new Point(1.5,9.0));
		points.add(new Point(1.5,8.5));
		points.add(new Point(1.0,9.5));
		KMeans kmeans = new KMeans(points, 3);
		kmeans.find();
	}

}