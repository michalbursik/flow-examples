<?php

namespace Examples\ProductsProcessing;

use Flow\ETL\DataFrame;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Join\Expression;

use Flow\ETL\Row;

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\equal;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\when;

class ProductsProcessing
{

    // Source XML file & its configuration
    const string XML_NODE_PATH = 'products/product';
    const string SOURCE_XML_FILEPATH_FROM_ROOT = 'Examples/ProductsProcessing/data/products_complex/new_products.xml';

    // Products inserted after source XML file is processed (DB creates IDs), used for joining with product variants
    const string EXISTING_PRODUCTS_AFTER_INSERT_RELATIVE_FILEPATH = 'data/products_complex/existing_products_after_insert.php';

    // Database mocks: existing products and product variants (e.g. from previous imports)
    // So far no need to use - its here for better imagination of how it will work
    const string EXISTING_PRODUCTS_RELATIVE_FILEPATH = 'data/products_complex/existing_products.php';
    const string EXISTING_PRODUCT_VARIANTS_RELATIVE_FILEPATH = 'data/products_complex/existing_product_variants.php';

    public function run()
    {
        // The goal is to generate CSV that I can use to load into MySql database (LOAD INFILE).
        // I want to insert new or update existing products in the database.
        // Also, I would like to get some number of what items were inserted, updated or skipped
        // - (Not able to get it from "LOAD INFILE" directly)

        // My idea is to join IDs of existing products from DB to new products loaded from the XML file.
        // Ideally count the records with id present => that would be count of updates
        // and count the records with id not present => that would be count of inserts
        // and somehow extract from filters the skipped items count

        // There are also two tables that I am creating:
        // products (product 1)
        // product_variants (product 1, blue, 150 EUR)(product 1, red, 155 EUR)(...)
        // product_variants are all received products
        // product I am creating by grouping the records by "GROUPING_KEY" column

        $this->processProducts();

        // Here I am storing the products into the database (for simplification skipping)

        $this->processProductVariants();

        // Here once again I am storing product variants into the database (for simplification skipping)
    }

    private function processProducts(): void
    {
        $newProductsDF = data_frame()->read(from_xml(self::SOURCE_XML_FILEPATH_FROM_ROOT, self::XML_NODE_PATH));

        $newProductsDF->withEntries([
            'EXTERNAL_ID' => ref('node')->xpath('merchant_product_id')->domElementValue(),
            'GROUP_ID' => ref('node')->xpath('parent_product_id')->domElementValue(),
            'NAME' => ref('node')->xpath('product_name')->domElementValue(),
            'IMAGE_URL' => ref('node')->xpath('merchant_image_url')->domElementValue(),
            'PRICE' => ref('node')->xpath('search_price')->domElementValue(),
            'CURRENCY' => ref('node')->xpath('currency')->domElementValue(),
            'URL' => ref('node')->xpath('merchant_deep_link')->domElementValue(),

            // Temp column, just for filtering out the products
            'IN_STOCK' => when(
                ref('node')->xpath('in_stock')->domElementValue()->isIn(lit(['Y', '1', 'Yes'])),
                lit(true),
                lit(false)
            ),

            'BRAND' => ref('node')->xpath('brand_name')->domElementValue(),
            'DESCRIPTION' => ref('node')->xpath('description')->domElementValue(),

            // Static columns - filling up manually
            'SHOP_ID' => lit(1),
            'FEED_ID' => lit(1),
            'UPDATED' => lit(date('Y-m-d H:i:s')),

            // Grouping key
            'GROUPING_KEY' => ref('node')->xpath('parent_product_id')->domElementValue(),

            // There will be more processing e.g.: (not implemented yet - still discovering how the ETL lib works)
            // - categories: "Man > Shirts > Polo" -> DB structure:
            //      Man category -> Shirts category (Man category id) -> Polo category (Shirts category id)"
            // - colors: Blue, Coquelicot (Filtering out only desired colors -> skip a variant with "Coquelicot" color)
            //      also collect some comma separated list of "allowed" colors for the "parent" product
            // - sizes: collect some comma separated list of sizes for the "parent" product
            // - parameters: collect useful parameters from XML for later use
            // - shipping: extra shipping table with name & cost columns

            // Also, later on I will need count of skipped items (through the filters)
            // categorized by the filter applied (Need to tweak the ETL lib for that)
        ]);

        $this->applyFilters($newProductsDF);

        $newProductsDF->dropDuplicates('GROUPING_KEY');

        // Simulates SQL query to DB
        $existingProducts = require_once self::EXISTING_PRODUCTS_RELATIVE_FILEPATH;
        $columnsToDrop = [];

        if (!empty($existingProducts)) {
            $existingProductsDF = data_frame()->read(from_array($existingProducts));
            $newProductsDF->join($existingProductsDF, Expression::on(equal('GROUPING_KEY', 'GROUPING_KEY')));

            $columnsToDrop = ['joined_ID', 'joined_GROUPING_KEY'];

            // Add the ID column based on the joined values -> ensures the products are updated instead of insert
            $newProductsDF->withEntry('ID',
                when(
                    ref('joined_ID')->equals(lit('')),
                    lit("NULL"),
                    ref('joined_ID')
                )
            );
        }

        $newProductsDF->drop('node', 'IN_STOCK', ...$columnsToDrop);
        $newProductsDF->saveMode(SaveMode::Overwrite);
        $newProductsDF->write(to_csv('./products_output.csv'));
        $newProductsDF->run();
    }

    private function processProductVariants(): void
    {
        $newProductVariantsDF = data_frame()->read(from_xml(self::SOURCE_XML_FILEPATH_FROM_ROOT, self::XML_NODE_PATH));

        $newProductVariantsDF->withEntries([
            'EXTERNAL_ID' => ref('node')->xpath('merchant_product_id')->domElementValue(),
            'GROUP_ID' => ref('node')->xpath('parent_product_id')->domElementValue(),
            'GTIN' => ref('node')->xpath('product_GTIN')->domElementValue(),
            'MPN' => ref('node')->xpath('mpn')->domElementValue(),
            'NAME' => ref('node')->xpath('product_name')->domElementValue(),
            'GENDER' => ref('node')->xpath('suitable_for')->domElementValue(),
            'IMAGE_URL' => ref('node')->xpath('merchant_image_url')->domElementValue(),
            'PRICE' => ref('node')->xpath('search_price')->domElementValue(),
            'CURRENCY' => ref('node')->xpath('currency')->domElementValue(),
            'SIZE' => ref('node')->xpath('size')->domElementValue(),
            'URL' => ref('node')->xpath('merchant_deep_link')->domElementValue(),
            'DIRECT_URL' => ref('node')->xpath('merchant_deep_link')->domElementValue(),
            'COLOR' => ref('node')->xpath('colour')->domElementValue(),

            // Temp column, just for filtering out the products
            'IN_STOCK' => when(
                ref('node')->xpath('in_stock')->domElementValue()->isIn(lit(['Y', '1', 'Yes'])),
                lit(true),
                lit(false)
            ),
            'BRAND' => ref('node')->xpath('brand_name')->domElementValue(),
            'DESCRIPTION' => ref('node')->xpath('description')->domElementValue(),

            'SHOP_ID' => lit(1),
            'FEED_ID' => lit(1),
            'UPDATED' => lit(date('Y-m-d H:i:s')),

            'GROUPING_KEY' => ref('node')->xpath('parent_product_id')->domElementValue(),
        ]);

        $this->applyFilters($newProductVariantsDF);

        // Connect product with product_variants through product_id (database relation)
        // Simulates SQL query to DB (SELECTS only column to group it by and id of a product - created by previous insert)
        $existingProductsAfterInsert = require_once self::EXISTING_PRODUCTS_AFTER_INSERT_RELATIVE_FILEPATH;

        // There should always be some existing product variant as products are created from product_variants
        $existingProductsDF = data_frame()->read(from_array($existingProductsAfterInsert));
        $newProductVariantsDF->join($existingProductsDF, Expression::on(equal('GROUPING_KEY', 'GROUPING_KEY')));
        $newProductVariantsDF->rename('joined_ID', 'PRODUCT_ID');
        // Need to drop since joining columns needs to be unique.
        $newProductVariantsDF->drop('joined_GROUPING_KEY');

        // Simulates SQL query to DB
        $existingProductVariants = require_once self::EXISTING_PRODUCT_VARIANTS_RELATIVE_FILEPATH;
        $columnsToDrop = [];
        if ($existingProductVariants) {
            $existingProductVariantsDF = data_frame()->read(from_array($existingProductVariants));
            $newProductVariantsDF->join($existingProductVariantsDF, Expression::on(equal('EXTERNAL_ID', 'EXTERNAL_ID')));

            // Add the ID column based on the joined values -> ensures the product variants are updated instead of insert
            $newProductVariantsDF->withEntry('ID',
                when(
                    ref('joined_ID')->equals(lit('')),
                    lit("NULL"),
                    ref('joined_ID')
                )
            );

            $columnsToDrop = ['joined_ID', 'joined_EXTERNAL_ID'];
        }

        $newProductVariantsDF->drop('node', 'IN_STOCK', ...$columnsToDrop);
        $newProductVariantsDF->saveMode(SaveMode::Overwrite);
        $newProductVariantsDF->write(to_csv('product_variants_output.csv', false));
        $newProductVariantsDF->run();
    }

    private function getFilters(): array
    {
        return [
            // Required fields
            ref('EXTERNAL_ID')->notEquals(lit('')),
            ref('NAME')->notEquals(lit('')),
            ref('URL')->notEquals(lit('')),
            ref('IMAGE_URL')->notEquals(lit('')),

            // Available
            ref('IN_STOCK')->isTrue(),

            // Price higher than 0
            ref('PRICE')->greaterThan(lit(0)),
        ];
    }

    private function applyFilters(DataFrame $dataFrame): void
    {
        $filters = $this->getFilters();
        foreach ($filters as $filter) {
            $dataFrame->filter($filter);
        }
    }
}