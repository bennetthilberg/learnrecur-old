import { clickNavigation } from "../support/navigation";
import { clerk } from "@clerk/testing/playwright";

import { expect, test } from "../fixtures/authenticated";

const hydrationError = /hydrat|Minified React error #(?:418|423|425)/i;

for (const viewport of [
  { name: "desktop", width: 1280, height: 900 },
  { name: "mobile", width: 390, height: 844 },
]) {
  test(`an authenticated learner can open the core application pages on ${viewport.name}`, async ({ page }, testInfo) => {
    test.setTimeout(90_000);
    await page.setViewportSize(viewport);
    const hydrationErrors: string[] = [];
    page.on("console", (message) => {
      if (message.type() === "error" && hydrationError.test(message.text())) {
        hydrationErrors.push(message.text());
      }
    });
    page.on("pageerror", (error) => {
      if (hydrationError.test(error.message)) hydrationErrors.push(error.message);
    });
    const pages = [
      { path: "/dashboard", heading: /due skill/i },
      { path: "/skills", heading: /^skills$/i },
      { path: "/collections", heading: /^collections$/i },
      { path: "/history", heading: /^history$/i },
      { path: "/settings", heading: /^settings$/i },
    ];

    for (const expected of pages) {
      await page.goto(expected.path);
      await expect(page).toHaveURL(new RegExp(`${expected.path.replace("/", "\\/")}$`));
      await expect(page.getByRole("heading", { name: expected.heading }).first()).toBeVisible();
      // Loading shells include the page title too; wait for the real content.
      await expect(page.locator(".routeSkeleton")).toHaveCount(0);
      // A heading can appear before Clerk finishes mounting the account widget.
      await clerk.loaded({ page });
      await expect(page.locator(".learnrecurUserButton")).toBeVisible();
      expect(hydrationErrors, `hydration errors on ${expected.path}`).toEqual([]);
    }

    const identity = page.getByRole("button", { name: /open account menu for/i });
    await (await identity.isVisible() ? identity : page.locator(".learnrecurUserButton")).click();
    await expect(page.getByRole("button", { name: /manage account/i })).toBeVisible();
    await expect(page.getByRole("button", { name: /sign out/i })).toBeVisible();
    await page.screenshot({
      path: testInfo.outputPath(`account-menu-${viewport.name}.png`),
      animations: "disabled",
    });

    await page.getByRole("button", { name: /manage account/i }).click();
    await page.getByRole("button", { name: "Security", exact: true }).click();
    await page.getByRole("button", { name: "Update password", exact: true }).click();
    const signOutCheckbox = page.locator("#signOutOfOtherSessions-field");
    await expect(signOutCheckbox).toBeVisible();
    await expect(signOutCheckbox).toHaveCSS("width", "18px");
    await expect(signOutCheckbox).toHaveCSS("height", "18px");
    const passwordFormCard = page.locator(".cl-cardBox").filter({ has: signOutCheckbox }).last();
    await expect(passwordFormCard).toHaveCSS("overflow", "hidden");
    await page.screenshot({
      path: testInfo.outputPath(`account-security-${viewport.name}.png`),
      animations: "disabled",
    });
    expect(hydrationErrors).toEqual([]);
  });
}

test("signing out removes access to protected pages", async ({ page }) => {
  await page.goto("/");
  await clerk.signOut({ page });

  await page.goto("/dashboard");
  await expect(page).toHaveURL(/\/sign-in|accounts\.dev\/sign-in/);
  await expect(page.getByRole("heading", { name: /due skill/i })).toHaveCount(0);
});

for (const viewport of [
  { name: "desktop", width: 1280, height: 900 },
  { name: "mobile", width: 390, height: 844 },
]) {
  test(`action feedback is a compact toast and navigation stays static on ${viewport.name}`, async ({ page }, testInfo) => {
    await page.setViewportSize(viewport);
    await page.goto("/skills?deletedSkill=1");
    const toast = page.locator(".learnrecurNotification").filter({ hasText: "Skill permanently deleted." });
    await expect(toast).toBeVisible();
    await expect(page.locator(".skillShell > .skillFormMessage")).toHaveCount(0);
    await expect(toast).toBeInViewport({ ratio: 1 });
    const box = await toast.boundingBox();
    expect(box).not.toBeNull();
    expect(Math.round(box!.width)).toBeLessThanOrEqual(360);
    expect(box!.x).toBeGreaterThanOrEqual(0);
    expect(box!.x + box!.width).toBeLessThanOrEqual(viewport.width);
    await page.screenshot({ path: testInfo.outputPath(`toast-${viewport.name}.png`) });
    await toast.getByRole("button").click();
    await expect(toast).toHaveCount(0);

    const nav = page.locator(".practiceNav");
    await clickNavigation(page, "Collections");
    await expect(page).toHaveURL(/\/collections$/);
    await expect(page.locator(".routeSkeleton")).toHaveCount(0);
    await expect(nav.locator('a[data-nav-key="collections"]')).toHaveAttribute("aria-current", "page");
    await expect(nav.locator('a[data-nav-key="collections"]')).toHaveAttribute("data-nav-active", "true");
    await expect(page.locator(".practiceNavActiveIndicator, .practiceNavFloatingIndicator")).toHaveCount(0);
    if (viewport.name === "desktop") {
      await expect(nav.locator('a[data-nav-key="collections"]')).toHaveCSS("background-color", "rgb(255, 255, 255)");
    }
    await page.screenshot({ path: testInfo.outputPath(`static-nav-${viewport.name}.png`) });
  });
}

test("collection form errors use dismissible toasts and repeat after retry", async ({ page }) => {
  await page.goto("/collections");
  const form = page.locator(".collectionCreateForm");
  const toast = page.locator(".learnrecurNotification").filter({ hasText: "Could not update collection" });
  for (let attempt = 0; attempt < 2; attempt += 1) {
    await form.getByRole("textbox", { name: /^Name\b/ }).fill("   ");
    await form.getByRole("button", { name: "Create collection", exact: true }).click();
    await expect(toast).toBeVisible();
    await expect(form.locator(".skillFormMessage")).toHaveCount(0);
    await expect(form.getByRole("textbox", { name: /^Name\b/ })).toHaveAttribute("aria-invalid", "true");
    await toast.getByRole("button").click();
    await expect(toast).toHaveCount(0);
  }
});


test("resizing the sidebar keeps the selected settings link visible", async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 900 });
  await page.goto("/settings");
  await expect(page.locator(".routeSkeleton")).toHaveCount(0);
  const nav = page.getByRole("navigation", { name: "Primary navigation" });
  const settings = nav.getByRole("link", { name: "Settings", exact: true });
  await expect(settings).toHaveAttribute("data-nav-active", "true");
  for (const width of [800, 600, 390, 1280, 375]) {
    await page.setViewportSize({ width, height: 844 });
    if (width < 1120) {
      await page.getByRole("button", { name: "Pages", exact: true }).click();
      await expect(page.getByRole("menuitem", { name: "Settings", exact: true })).toHaveAttribute("aria-current", "page");
      await page.keyboard.press("Escape");
    } else await expect(settings).toBeInViewport();
    await expect(page.locator(".practiceNavActiveIndicator, .practiceNavFloatingIndicator")).toHaveCount(0);
  }
});

test("a corrected collection submission replaces the undismissed error toast", async ({ page }) => {
  await page.goto("/collections");
  const form = page.locator(".collectionCreateForm");
  const name = form.getByRole("textbox", { name: /^Name\b/ });
  const submit = form.getByRole("button", { name: "Create collection", exact: true });
  await name.fill("   ");
  await submit.click();
  const toasts = page.locator(".learnrecurNotification");
  await expect(toasts).toContainText("Could not update collection");
  await name.fill(`Toast retry ${Date.now()}`);
  await submit.click();
  await expect(toasts).toHaveCount(1);
  await expect(toasts).toContainText("Collection created.");
  await expect(toasts).not.toContainText("Could not update collection");
});
