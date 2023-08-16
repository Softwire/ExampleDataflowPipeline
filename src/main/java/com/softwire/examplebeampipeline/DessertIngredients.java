package com.softwire.examplebeampipeline;

import java.util.stream.StreamSupport;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;


public class DessertIngredients {

  @DefaultCoder(AvroCoder.class)
  public static class Ingredient {
    private String name;
    private int quantity;

    public Ingredient() {
    }
    
    public Ingredient(String name, int quantity) {
      this.name = name;
      this.quantity = quantity;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getQuantity() {
      return quantity;
    }

    public void setQuantity(int quantity) {
      this.quantity = quantity;
    }
  }
   
  @DefaultCoder(AvroCoder.class)
  public static class Sale {
    private String productName;
    private int quantity;
    private String storeName;

    public Sale() {
    }

    public Sale(String productName, int quantity, String storeName) {
      this.productName = productName;
      this.quantity = quantity;
      this.storeName = storeName;
    }

    public String getProductName() {
      return productName;
    }
    public void setProductName(String productName) {
      this.productName = productName;
    }
    public int getQuantity() {
      return quantity;
    }
    public void setQuantity(int quantity) {
      this.quantity = quantity;
    }
    public String getStoreName() {
      return storeName;
    }
    public void setStoreName(String storeName) {
      this.storeName = storeName;
    }
  }

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
    Pipeline pipeline = Pipeline.create(options);

    // Create some dataset to process.  In a real pipeline, we'd probably be reading these 
    // from a bucket or API somewhere.
    PCollection<Sale> productSales =
        pipeline.apply("Read Product Sales", Create.of(
            new Sale("apple crumble", 17, "London"),
            new Sale("apple crumble", 2, "New York"),
            new Sale("blackberry crumble", 2, "Paris"),
            new Sale("apple", 2, "Berlin")
        ));

    PCollection<KV<String, Ingredient>> ingredients =
        pipeline.apply(
            "Read Ingredients",
            Create.of(
                // Apple Crumble Ingredients
                KV.of("apple crumble", new Ingredient("apple", 8)),
                KV.of("apple crumble", new Ingredient("flour", 200)),
                KV.of("apple crumble", new Ingredient("butter", 150)),
                KV.of("apple crumble", new Ingredient("sugar", 150)),

                // Blackberry Crumble Ingredients
                KV.of("blackberry crumble", new Ingredient("blackberry", 400)),
                KV.of("blackberry crumble", new Ingredient("flour", 200)),
                KV.of("blackberry crumble", new Ingredient("butter", 150)),
                KV.of("blackberry crumble", new Ingredient("sugar", 150)),

                // Apple Ingredients
                KV.of("apple", new Ingredient("apple",1))
              ));

    // Product Sales aren't yet of the right form to link them with ingredients: we need them to be
    // as key-value pairs.
    PCollection<KV<String, Integer>> salesByProduct = productSales.apply("To SalesByProduct", ParDo.of(new DoFn<Sale, KV<String, Integer>>() {
        @ProcessElement
        public void processElement(ProcessContext processContext) {
          final var sale = processContext.element();

          processContext.output(KV.of(sale.productName, sale.quantity));
        }
    }));

    // Join our new key-value pair of sales by product with the ingredients list using CoGroupByKey.
    final TupleTag<Integer> salesByProductTag = new TupleTag<>();
    final TupleTag<Ingredient> ingredientsTag = new TupleTag<>();
    PCollection<KV<String, CoGbkResult>> result =
        KeyedPCollectionTuple.of(salesByProductTag, salesByProduct).and(ingredientsTag, ingredients).apply("Group by Product", CoGroupByKey.create());

    // Calculate the ingredients required from each sale
    PCollection<String> ingredientsRequired = result.apply("Calculate Ingredients", ParDo.of(new DoFn<KV<String,CoGbkResult>, String>() {
        @ProcessElement
        public void processElement(ProcessContext processContext) {
          final KV<String, CoGbkResult> groupedElement = processContext.element();

          // We don't need the explicit types here, but I'll include them because it's easier to read when you're not inside an IDE.
          final Iterable<Integer> salesByProduct = groupedElement.getValue().getAll(salesByProductTag);
          final Iterable<Ingredient> ingredients = groupedElement.getValue().getAll(ingredientsTag);
          
          // Calculate the total sales for the current product and then use that to calculate ingredients required
          final var totalSales = StreamSupport.stream(salesByProduct.spliterator(), false).reduce(0, Integer::sum); 
          for (final var ingredient : ingredients) {
            processContext.output(ingredient.name + ": " + ingredient.quantity * totalSales);
          }
        }
    }));

    ingredientsRequired.apply("Write Out", TextIO.write().to("gs://dfeg-396110/output/ingredients.txt"));

    pipeline.run();
  }

}
