from pyspark import SparkContext
from collections import defaultdict

def load_data(sc, user_action_file, product_category_file):
    """Load user behavior data and product category data"""
    user_action_rdd = sc.textFile(user_action_file)
    product_category_rdd = sc.textFile(product_category_file)
    return user_action_rdd, product_category_rdd

def parse_data(user_action_rdd, product_category_rdd):
    """Parse data"""
    user_action_parsed = user_action_rdd.map(lambda line: line.split(','))
    product_category_parsed = product_category_rdd.map(lambda line: line.split(','))
    return user_action_parsed, product_category_parsed

def merge_data(user_action_parsed, product_category_parsed):
    """Merge user behavior data and product category data"""
    kv_product_category_parsed = product_category_parsed.map(lambda x: (x[0], x[1]))
    # (product_name, category)
    # (product_name, (user_name, action))
    # (product_name, ((user_name, action), category)) 
    merged_data = user_action_parsed.map(lambda x: (x[1], (x[0], x[2]))).join(kv_product_category_parsed).map(lambda x: (x[1][0][0], x[0], x[1][1], x[1][0][1]))
    # (user_name, product_name, category, action)
    print("Merged data: (user_name, product_name, category, action)")
    merged_data.foreach(lambda x: print(x))
    return merged_data

def filter_data(merged_data):
    """Filter out records that are not purchases, favorites, view details or add to cart"""
    to_keep = ['purchase', 'favorite', 'add_to_cart', 'view_details']
    filtered_data = merged_data.filter(lambda x: x[3] in to_keep)
    
    # (user_name, product_name, category, action)
    print("Filtered data: (user_name, product_name, category, action)")
    filtered_data.foreach(lambda x: print(x))
    return filtered_data

def analyze_user_behavior(filtered_data):
    """Analyze user behavior to determine the most frequently interacted products"""
    # (user_name, product_name, action)
    # ((user_name, product_name), 1)
    user_product_counts = filtered_data.map(lambda x: ((x[0], x[1]), 1)).reduceByKey(lambda a, b: a + b)
    # ((user_name, product_name), n)

    # (user_name, [(product_name1, n), (product_name2, n), ...])
    # (user_name, max_count)
    user_max_count = user_product_counts.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey().mapValues(lambda x: max(x, key=lambda y: y[1])[1])
    # (user_name, (product_name, n))
    # (user_name, ((product_name, n), max_count))
    # (user_name, product_name)
    top_products = user_product_counts.map(lambda x: (x[0][0], (x[0][1], x[1]))).join(user_max_count).filter(lambda x: x[1][0][1] == x[1][1]).map(lambda x: (x[0], x[1][0][0]))
    # (user_name, product_name)
    # (user_name, [product_name])
    top_products = top_products.groupByKey().mapValues(list)

    print("top products: (user_name, [product_name])")
    top_products.foreach(lambda x: print(x))
    return top_products


def generate_recommendations(user_most_interested, product_category_parsed):
    """Generate recommendations"""
    # get the key value pair p2c 
    product_category_kv = product_category_parsed.map(lambda x: (x[0], x[1]))
    # (user_name, [product_name])
    # (user_name, category, product)
    recommendations = user_most_interested.flatMap(
        lambda x: [(prod, x[0]) for prod in x[1]]  # (product, user)
    ).join(
        product_category_kv  # Join with (product, category)
    ).map(
        lambda x: (x[1][0], x[1][1], x[0])  # (user, category, product)
    )

    recommendations = recommendations.map(
        lambda x: ((x[0], x[1]), [x[2]])  # ((user, category), [product])
    ).reduceByKey(
        lambda a, b: a + b 
    ).map(
        lambda x: (x[0][0], x[0][1], sorted(x[1]))  # (user, category, [sorted_products])
    ).sortBy(
        lambda x: (x[0], x[1])
    )
    print("Recommendations")
    recommendations.foreach(lambda x: print(x))

    return recommendations

def output_recommendations(recommendations):
    """Output recommendation results"""
    for user, category, products in recommendations.collect():
        print(f"{user}: {category} -> {products}")

def main(sc, user_action_file, product_category_file):
    """Main function, integrate all steps"""
    # 1. Load data
    user_action_rdd, product_category_rdd = load_data(sc, user_action_file, product_category_file)
    
    # 2. Parse data
    user_action_parsed, product_category_parsed = parse_data(user_action_rdd, product_category_rdd)
    
    # 3. Merge data
    merged_data = merge_data(user_action_parsed, product_category_parsed)
    
    # 4. Filter data
    filtered_data = filter_data(merged_data)
    
    # 5. Analyze user behavior
    user_most_interested = analyze_user_behavior(filtered_data)
    
    # 6. Generate recommendations
    #recommendations = generate_recommendations(user_most_interested)
    recommendations = generate_recommendations(user_most_interested, product_category_parsed)
    
    # 7. Output recommendation results
    output_recommendations(recommendations)

# Initialize SparkContext
sc = SparkContext("local", "Recommendation System")

# Call main function
main(sc, 'user_actions_test.csv', 'product_categories_test.csv')

# Stop SparkContext
sc.stop()