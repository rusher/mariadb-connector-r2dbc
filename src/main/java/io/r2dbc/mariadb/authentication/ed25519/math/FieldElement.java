/**
 * EdDSA-Java by str4d
 *
 * <p>To the extent possible under law, the person who associated CC0 with EdDSA-Java has waived all
 * copyright and related or neighboring rights to EdDSA-Java.
 *
 * <p>You should have received a copy of the CC0 legalcode along with this work. If not, see
 * <https://creativecommons.org/publicdomain/zero/1.0/>.
 */
package io.r2dbc.mariadb.authentication.ed25519.math;

import java.io.Serializable;

/** Note: concrete subclasses must implement hashCode() and equals() */
public abstract class FieldElement implements Serializable {

  private static final long serialVersionUID = 1239527465875676L;

  protected final Field f;

  public FieldElement(Field f) {
    if (null == f) {
      throw new IllegalArgumentException("field cannot be null");
    }
    this.f = f;
  }

  /**
   * Encode a FieldElement in its $(b-1)$-bit encoding.
   *
   * @return the $(b-1)$-bit encoding of this FieldElement.
   */
  public byte[] toByteArray() {
    return f.getEncoding().encode(this);
  }

  public abstract boolean isNonZero();

  public boolean isNegative() {
    return f.getEncoding().isNegative(this);
  }

  public abstract io.r2dbc.mariadb.authentication.ed25519.math.FieldElement add(
      io.r2dbc.mariadb.authentication.ed25519.math.FieldElement val);

  public io.r2dbc.mariadb.authentication.ed25519.math.FieldElement addOne() {
    return add(f.ONE);
  }

  public abstract io.r2dbc.mariadb.authentication.ed25519.math.FieldElement subtract(
      io.r2dbc.mariadb.authentication.ed25519.math.FieldElement val);

  public io.r2dbc.mariadb.authentication.ed25519.math.FieldElement subtractOne() {
    return subtract(f.ONE);
  }

  public abstract io.r2dbc.mariadb.authentication.ed25519.math.FieldElement negate();

  public io.r2dbc.mariadb.authentication.ed25519.math.FieldElement divide(
      io.r2dbc.mariadb.authentication.ed25519.math.FieldElement val) {
    return multiply(val.invert());
  }

  public abstract io.r2dbc.mariadb.authentication.ed25519.math.FieldElement multiply(
      io.r2dbc.mariadb.authentication.ed25519.math.FieldElement val);

  public abstract io.r2dbc.mariadb.authentication.ed25519.math.FieldElement square();

  public abstract io.r2dbc.mariadb.authentication.ed25519.math.FieldElement squareAndDouble();

  public abstract io.r2dbc.mariadb.authentication.ed25519.math.FieldElement invert();

  public abstract io.r2dbc.mariadb.authentication.ed25519.math.FieldElement pow22523();

  public abstract io.r2dbc.mariadb.authentication.ed25519.math.FieldElement cmov(
      io.r2dbc.mariadb.authentication.ed25519.math.FieldElement val, final int b);

  // Note: concrete subclasses must implement hashCode() and equals()
}
