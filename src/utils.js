// TODO: Should move to not need this, only pass back PINs instead of full props
const METERS_IN_MILE = 1609.344

export const DISPLAY_FIELDS = [
  {
    title: "Address",
    field: "address",
  },
  {
    title: "Age",
    field: "age",
  },
  {
    title: "Stories (Not Including Basement)",
    field: "stories_display",
  },
  {
    title: "Exterior",
    field: "exterior_display",
  },
  {
    title: "Baths",
    field: "baths_display",
  },
  {
    title: "Garage",
    field: "garage_display",
  },
  {
    title: "Basement",
    field: "basement_display",
  },
  {
    title: "Distance",
    field: "distance_display",
  },
  {
    title: "Neighborhood",
    field: "neighborhood",
  },
  {
    title: "Assessor Market Value",
    field: "assessed_value_display",
  },
  {
    title: "Sale Price",
    field: "sale_price_display",
  },
  {
    title: "Sale Date",
    field: "sale_date",
  },
]

export const DISPLAY_FIELDS_COOK = [
  {
    title: "Address",
    field: "address",
  },
  {
    title: "Age",
    field: "age",
  },
  {
    title: "Basement",
    field: "basement_display",
  },
  {
    title: "Beds",
    field: "bedrooms",
  },
  {
    title: "Building Sq Ft.",
    field: "building_sq_ft",
  },
  {
    title: "Land Sq Ft.",
    field: "land_sq_ft",
  },
  {
    title: "Class",
    field: "property_class",
  },
  {
    title: "Exterior",
    field: "wall_material_display",
  },
  {
    title: "Garage",
    field: "garage_display",
  },
  {
    title: "Rooms",
    field: "rooms",
  },
  {
    title: "Stories",
    field: "stories",
  },
  {
    title: "Distance",
    field: "distance_display",
  },
]

/* eslint-disable no-prototype-builtins */
export function cleanParcel(parcel) {
  const usd = new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  })
  if (parcel.street_number && parcel.street_name) {
    parcel.address = `${parcel.street_number} ${parcel.street_name}`
  }
  if (parcel.hasOwnProperty("exterior_category")) {
    parcel.exterior_display = {
      1: "Siding",
      2: "Brick/other",
      3: "Brick",
      4: "Other",
    }[parcel.exterior_category]
  }
  if (parcel.hasOwnProperty("wall_material")) {
    parcel.wall_material_display = {
      1: "Wood",
      2: "Masonry",
      3: "Wood/Masonry",
      4: "Stucco",
    }[+parcel.wall_material]
  }
  if (parcel.hasOwnProperty("baths")) {
    parcel.baths_display = {
      1: "1",
      2: "1.5",
      3: "2 to 3",
      4: "3+",
    }[parcel.baths]
  }
  if (parcel.hasOwnProperty("assessed_value")) {
    parcel.assessed_value_display = usd.format(parcel.assessed_value * 2)
  }
  if (parcel.sale_price) {
    parcel.sale_price_display = usd.format(parcel.sale_price)
  }
  if (parcel.hasOwnProperty("distance")) {
    parcel.distance_display = `${(parcel.distance / METERS_IN_MILE).toFixed(
      2
    )} mi`
  }
  if (parcel.hasOwnProperty("stories")) {
    parcel.stories_display = {
      1: "1 to 1.5",
      2: "1.5 to 2.5",
      3: "3+",
    }[parcel.stories]
  }
  if (parcel.hasOwnProperty("basement")) {
    parcel.basement_display = parcel.hasOwnProperty("wall_material")
      ? parcel.basement
        ? "Full"
        : "Partial/None"
      : parcel.basement
      ? "Yes"
      : "None"
  }
  if (parcel.hasOwnProperty("garage")) {
    parcel.garage_display = parcel.garage ? "Yes" : "None"
  }
  return parcel
}
